import aws_cdk as cdk
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


class InfrastructureStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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

        # S3 Gateway Endpoint
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # ==== S3 Bronze Bucket ==== 
        bronze_bucket = s3.Bucket(
            self,
            "BronzeBucket",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(14),
                )
            ],
        )

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

        # role for twitter
        twitter_role = iam.Role(
            self,
            "TwitterRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"), 
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # allow twitter role to put and read data from the bronze bucket
        bronze_bucket.grant_put(twitter_role)
        bronze_bucket.grant_read(twitter_role)

        # ==== Lambdas =====

        hacker_news_fn = lambda_.Function(
            self,
            "HackerNewsIngestor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../app/bronze/hacker_news"),
            role=lambda_role,
            memory_size=128,
            timeout=Duration.minutes(5),
            environment={
                "BRONZE_BUCKET": bronze_bucket.bucket_name,
                "MAX_PUTS": "500",  # set a 0 for unlimited S3 PUTs
            },
        )

        # lambda for checking whether the twitter file has been uploaded to s3 or not
        twitter_fn = lambda_.Function(self, "TwitterIngestor", runtime=lambda_.Runtime.PYTHON_3_12, handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../app/bronze/twitter"), role=twitter_role, memory_size=128, timeout=Duration.minutes(1),
            environment={
                "BRONZE_TWITTER_BUCKET": bronze_bucket.bucket_name,
            },
        )

        # EventBridge Daily Schedule
        # Runs at 01:00 UTC every day
        # events.Rule(
        #     self,
        #     "DailyHackerNewsSchedule",
        #     schedule=events.Schedule.cron(minute="0", hour="1"),
        #     targets=[targets.LambdaFunction(hacker_news_fn)],
        # )
