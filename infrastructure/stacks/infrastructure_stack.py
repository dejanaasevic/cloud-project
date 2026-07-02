import aws_cdk as cdk
from aws_cdk import aws_lambda_destinations as destinations
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    RemovalPolicy,
)
from constructs import Construct
import os
import boto3
import botocore.exceptions


class InfrastructureStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # self.account/self.region are unresolved CDK tokens for an environment-agnostic
        # stack, so resolve concrete values directly via boto3 for the synth-time bucket check.
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        region_name = boto3.session.Session().region_name or self.region

        def get_or_create_bucket(bucket_construct_id: str, bucket_name: str, **bucket_kwargs) -> s3.IBucket:
            """Import the bucket if it already exists in this account/region, otherwise create it.

            Bucket names are globally reserved, and these buckets use RemovalPolicy.RETAIN,
            so a stack that was deleted and redeployed would otherwise collide with its own
            leftover buckets from a previous deploy.
            """
            s3_client = boto3.client("s3", region_name=region_name)
            try:
                s3_client.head_bucket(Bucket=bucket_name)
                print(f"Bucket {bucket_name} already exists, importing it.")
                return s3.Bucket.from_bucket_name(self, bucket_construct_id, bucket_name)
            except botocore.exceptions.ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code not in ("404", "NoSuchBucket"):
                    raise
                print(f"Bucket {bucket_name} does not exist, creating it.")
                return s3.Bucket(self, bucket_construct_id, bucket_name=bucket_name, **bucket_kwargs)

        # ==== VPC ==== 
        vpc = ec2.Vpc(
            self,
            "MainVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        self.vpc = vpc

        # S3 Gateway Endpoint
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # AWS-managed prefix list for the S3 gateway endpoint, used to scope security group
        # egress to "S3 in this region" instead of a broad 0.0.0.0/0 rule.
        ec2_client = boto3.client("ec2", region_name=region_name)
        s3_prefix_list_id = ec2_client.describe_managed_prefix_lists(
            Filters=[{"Name": "prefix-list-name", "Values": [f"com.amazonaws.{region_name}.s3"]}]
        )["PrefixLists"][0]["PrefixListId"]

        def create_s3_only_security_group(sg_construct_id: str, description: str) -> ec2.SecurityGroup:
            """A security group for VPC-isolated Lambdas that only ever talk to S3.

            No ingress (Lambda doesn't accept inbound network connections) and egress
            is limited to HTTPS toward the S3 prefix list, since these functions have
            no other network dependency and sit in a subnet with no NAT/IGW route anyway.
            """
            security_group = ec2.SecurityGroup(
                self,
                sg_construct_id,
                vpc=vpc,
                description=description,
                allow_all_outbound=False,
            )
            security_group.add_egress_rule(
                peer=ec2.Peer.prefix_list(s3_prefix_list_id),
                connection=ec2.Port.tcp(443),
                description="HTTPS to S3 gateway endpoint only",
            )
            return security_group

        isolated_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED)

        # ==== S3 Bronze Bucket ====
        bronze_bucket = get_or_create_bucket(
            "BronzeBucket",
            f"cloud-project-bronze-{account_id}-{region_name}",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(14),
                )
            ],
        )

        # add discord notifier role for failed jobs monitoring
        notifier_role = iam.Role(self, "DiscordNotifierRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                                 managed_policies=[
                                     iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
                                 ],
        )
        
        # discord notifier function for failed jobs monitoring
        notifier_fn = lambda_.Function(self, "DiscordNotifier", runtime=lambda_.Runtime.PYTHON_3_12, handler="handler.lambda_handler", 
                                    code=lambda_.Code.from_asset("../app/notifications/discord_notifier"), role=notifier_role,
                                    memory_size=512,  timeout=Duration.minutes(10),  
                                    environment={
                                        "DISCORD_WEBHOOK_URL": os.environ.get("DISCORD_WEBHOOK_URL", ""),
                                    },
        )
        self.notifier_fn = notifier_fn


        lambda_role = iam.Role(
            self,
            "HackerNewsLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        bronze_bucket.grant_put(lambda_role)

        # role for bronze twitter layer
        bronze_twitter_role = iam.Role(
            self, "BronzeTwitterRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"), 
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
        )

        # allow bronze twitter role to put and read data from the bronze bucket
        bronze_bucket.grant_put(bronze_twitter_role)
        bronze_bucket.grant_read(bronze_twitter_role)

        # ==== Lambdas =====

        hacker_news_fn = lambda_.Function(
            self,
            "HackerNewsIngestor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../app/bronze/hacker_news"),
            role=lambda_role,
            memory_size=128,
            timeout=Duration.minutes(10), 
            on_failure=destinations.LambdaDestination(notifier_fn),
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
                "MAX_PUTS": "500",  # set a 0 for unlimited S3 PUTs
            },
        )
        
        # add layer for pandas importing since its not included in the default aws lambda environment
        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(self, "PandasLayer",
                                                                   "arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python312:27",)
        
        self.pandas_layer = pandas_layer
        
        # add lambda fucntion for twitter data ingestion and filtering
        bronze_twitter_fn = lambda_.Function(self, "TwitterIngestor", runtime=lambda_.Runtime.PYTHON_3_12, handler="handler.lambda_handler", 
                                    code=lambda_.Code.from_asset("../app/bronze/twitter"), role=bronze_twitter_role, layers=[pandas_layer],
                                    ephemeral_storage_size=cdk.Size.mebibytes(1024),
                                    memory_size=512,  timeout=Duration.minutes(10),  
                                    on_failure=destinations.LambdaDestination(notifier_fn),
                                    retry_attempts=0,
                                    environment={
                                        "BRONZE_TWITTER_BUCKET": bronze_bucket.bucket_name,
                                        "KAGGLE_KEY": os.environ.get("KAGGLE_KEY", ""),
                                        "KAGGLE_USERNAME": os.environ.get("KAGGLE_USERNAME", ""),
                                    },
        )

        # bronze twitter event bridge - 01:00 UTC
        events.Rule(self, "DailyBronzeTwitterSchedule", schedule=events.Schedule.cron(minute="0", hour="1"), targets=[targets.LambdaFunction(bronze_twitter_fn)],)

        # EventBridge Daily Schedule
        # Runs at 01:00 UTC every day
        # events.Rule(
        #     self,
        #     "DailyHackerNewsSchedule",
        #     schedule=events.Schedule.cron(minute="0", hour="1"),
        #     targets=[targets.LambdaFunction(hacker_news_fn)],
        # )

        # silver bucket
        silver_bucket = get_or_create_bucket(
            "SilverBucket",
            f"cloud-project-silver-{account_id}-{region_name}",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(14),)
            ],
        )

        # allow silver twitter role to read from the bronze bucket and read/write to the silver bucket
        silver_twitter_role = iam.Role(
            self, "SilverTwitterRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
            ],
        )

        bronze_bucket.grant_read(silver_twitter_role)
        silver_bucket.grant_read_write(silver_twitter_role)

        silver_twitter_sg = create_s3_only_security_group(
            "SilverTwitterSecurityGroup", "SilverTwitterProcessor: HTTPS to S3 only"
        )

        # add lambda fucntion for twitter data normalization
        silver_twitter_fn = lambda_.Function(self, "SilverTwitterProcessor", runtime=lambda_.Runtime.PYTHON_3_12, handler="handler.lambda_handler",
                                    code=lambda_.Code.from_asset("../app/silver/twitter"), role=silver_twitter_role, layers=[pandas_layer],
                                    memory_size=512,  timeout=Duration.minutes(5),
                                    vpc=vpc, vpc_subnets=isolated_subnets, security_groups=[silver_twitter_sg],
                                    on_failure=destinations.LambdaDestination(notifier_fn), 
                                    environment={
                                        "BRONZE_TWITTER_BUCKET": bronze_bucket.bucket_name,
                                        "SILVER_TWITTER_BUCKET": silver_bucket.bucket_name,
                                    },
        )

        # silver twitter event bridge - 02:00 UTC
        events.Rule(self, "DailySilverTwitterSchedule", schedule=events.Schedule.cron(minute="0", hour="2"),targets=[targets.LambdaFunction(silver_twitter_fn)],)

        # ==== Silver Hacker News ====

        silver_hn_role = iam.Role(
            self, "SilverHackerNewsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
            ],
        )

        bronze_bucket.grant_read(silver_hn_role)
        silver_bucket.grant_read_write(silver_hn_role)

        silver_hn_sg = create_s3_only_security_group(
            "SilverHackerNewsSecurityGroup", "SilverHackerNewsProcessor: HTTPS to S3 only"
        )

        silver_hn_fn = lambda_.Function(
            self, "SilverHackerNewsProcessor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../app/silver/hacker_news"),
            role=silver_hn_role,
            layers=[pandas_layer],
            memory_size=512,
            timeout=Duration.minutes(10),
            vpc=vpc,
            vpc_subnets=isolated_subnets,
            security_groups=[silver_hn_sg],
            on_failure=destinations.LambdaDestination(notifier_fn),
            environment={
                "BRONZE_HN_BUCKET": bronze_bucket.bucket_name,
                "SILVER_HN_BUCKET": silver_bucket.bucket_name,
            },
        )

        # silver HN event bridge
        # events.Rule(
        #     self, "DailySilverHackerNewsSchedule",
        #     schedule=events.Schedule.cron(minute="0", hour="3"),
        #     targets=[targets.LambdaFunction(silver_hn_fn)],
        # )

        # ==== Gold Layer ====

        gold_bucket = get_or_create_bucket(
            "GoldBucket",
            f"cloud-project-gold-{account_id}-{region_name}",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(14)),
            ],
        )

        self.gold_bucket = gold_bucket

        gold_role = iam.Role(
            self, "GoldProcessorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
            ],
        )

        silver_bucket.grant_read(gold_role)
        gold_bucket.grant_read_write(gold_role)

        gold_sg = create_s3_only_security_group(
            "GoldProcessorSecurityGroup", "GoldProcessor: HTTPS to S3 only"
        )

        gold_fn = lambda_.Function(
            self, "GoldProcessor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../app/gold"),
            role=gold_role,
            layers=[pandas_layer],
            memory_size=512,
            timeout=Duration.minutes(10),
            vpc=vpc,
            vpc_subnets=isolated_subnets,
            security_groups=[gold_sg],
            on_failure=destinations.LambdaDestination(notifier_fn),
            environment={
                "SILVER_BUCKET": silver_bucket.bucket_name,
                "GOLD_BUCKET": gold_bucket.bucket_name,
            },
        )

        # gold event bridge - 04:00 UTC, after both silver jobs (02:00, 03:00) have run
        events.Rule(
            self, "DailyGoldSchedule",
            schedule=events.Schedule.cron(minute="0", hour="4"),
            targets=[targets.LambdaFunction(gold_fn)],
        )


