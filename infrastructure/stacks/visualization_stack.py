import aws_cdk as cdk
from aws_cdk import aws_lambda_destinations as destinations

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class VisualizationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        gold_bucket: s3.IBucket,
        pandas_layer: lambda_.ILayerVersion,
        notifier_function: lambda_.IFunction,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        superset_image = "apache/superset:6.0.0"

        superset_allowed_cidr = cdk.CfnParameter(
            self,
            "SupersetAllowedCidr",
            type="String",
            default="127.0.0.1/32",
            allowed_pattern=(
                r"^(?:\d{1,3}\.){3}\d{1,3}/"
                r"(?:[0-9]|[12][0-9]|3[0-2])$"
            ),
            description=(
                "Public IPv4 CIDR allowed to access Superset "
                "on port 8088. Use your public IP followed by /32."
            ),
        )

        # =============== Secrets Manager ===============

        database_secret = secretsmanager.Secret(
            self,
            "AnalyticsDatabaseSecret",
            description=(
                "Credentials for the analytics PostgreSQL database."
            ),
            generate_secret_string=(
                secretsmanager.SecretStringGenerator(
                    secret_string_template=(
                        '{"username":"analytics_user",'
                        '"database":"analytics"}'
                    ),
                    generate_string_key="password",
                    password_length=24,
                    exclude_punctuation=True,
                )
            ),
        )

        superset_admin_secret = secretsmanager.Secret(
            self,
            "SupersetAdminSecret",
            description="Apache Superset administrator credentials.",
            generate_secret_string=(
                secretsmanager.SecretStringGenerator(
                    secret_string_template=(
                        '{"username":"admin",'
                        '"email":"admin@cloud-project.local"}'
                    ),
                    generate_string_key="password",
                    password_length=20,
                    exclude_punctuation=True,
                )
            ),
        )

        superset_application_secret = secretsmanager.Secret(
            self,
            "SupersetApplicationSecret",
            description="Internal Apache Superset application secret.",
            generate_secret_string=(
                secretsmanager.SecretStringGenerator(
                    password_length=48,
                    exclude_punctuation=True,
                )
            ),
        )

        # =============== Security groups ===============

        lambda_security_group = ec2.SecurityGroup(
            self,
            "VisualizationLambdaSecurityGroup",
            vpc=vpc,
            description=(
                "Security group for the Gold-to-PostgreSQL Lambda."
            ),
            allow_all_outbound=True,
        )

        server_security_group = ec2.SecurityGroup(
            self,
            "VisualizationServerSecurityGroup",
            vpc=vpc,
            description=(
                "Security group for PostgreSQL and Superset on EC2."
            ),
            allow_all_outbound=True,
        )

        server_security_group.add_ingress_rule(
            peer=lambda_security_group,
            connection=ec2.Port.tcp(5432),
            description=(
                "Allow PostgreSQL traffic only from "
                "visualization Lambda."
            ),
        )

        server_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(
                superset_allowed_cidr.value_as_string
            ),
            connection=ec2.Port.tcp(8088),
            description=(
                "Allow Superset UI from the explicitly supplied CIDR."
            ),
        )

        endpoint_security_group = ec2.SecurityGroup(
            self,
            "SecretsManagerEndpointSecurityGroup",
            vpc=vpc,
            description=(
                "Restrict Secrets Manager endpoint access to the Lambda."
            ),
            allow_all_outbound=True,
        )

        endpoint_security_group.add_ingress_rule(
            peer=lambda_security_group,
            connection=ec2.Port.tcp(443),
            description=(
                "Allow visualization Lambda to read "
                "database credentials."
            ),
        )
        
        endpoint_security_group.add_ingress_rule(
            peer=server_security_group,
            connection=ec2.Port.tcp(443),
            description=(
                "Allow visualization EC2 server to read "
                "database and Superset credentials."
            ),
        )

        ec2.InterfaceVpcEndpoint(
            self,
            "SecretsManagerEndpoint",
            vpc=vpc,
            service=(
                ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER
            ),
            subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
            ),
            security_groups=[
                endpoint_security_group,
            ],
            private_dns_enabled=True,
            open=False,
        )

        # =============== EC2 role ===============


        server_role = iam.Role(
            self,
            "VisualizationServerRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )

        database_secret.grant_read(server_role)
        superset_admin_secret.grant_read(server_role)
        superset_application_secret.grant_read(server_role)

        # =============== EC2 user-data ===============


        user_data = ec2.UserData.for_linux()

        user_data.add_commands(
            "#!/bin/bash",
            "set -euo pipefail",
            (
                "exec > >(tee "
                "/var/log/visualization-setup.log) 2>&1"
            ),
            'echo "Starting visualization server setup version 4..."',
            "dnf update -y",
            "dnf install -y docker jq",
            "systemctl enable --now docker",
            "fallocate -l 2G /swapfile",
            "chmod 600 /swapfile",
            "mkswap /swapfile",
            "swapon /swapfile",
            'echo "/swapfile swap swap defaults 0 0" >> /etc/fstab',
            "usermod -aG docker ec2-user",
            f'AWS_REGION="{self.region}"',
            f'DB_SECRET_ARN="{database_secret.secret_arn}"',
            (
                f'SUPERSET_ADMIN_SECRET_ARN="'
                f'{superset_admin_secret.secret_arn}"'
            ),
            (
                f'SUPERSET_APP_SECRET_ARN="'
                f'{superset_application_secret.secret_arn}"'
            ),
            (
                'DB_SECRET=$(aws secretsmanager get-secret-value '
                '--secret-id "$DB_SECRET_ARN" '
                '--region "$AWS_REGION" '
                '--query SecretString '
                '--output text)'
            ),
            (
                'SUPERSET_ADMIN_SECRET=$('
                'aws secretsmanager get-secret-value '
                '--secret-id "$SUPERSET_ADMIN_SECRET_ARN" '
                '--region "$AWS_REGION" '
                '--query SecretString '
                '--output text)'
            ),
            (
                'SUPERSET_SECRET_KEY=$('
                'aws secretsmanager get-secret-value '
                '--secret-id "$SUPERSET_APP_SECRET_ARN" '
                '--region "$AWS_REGION" '
                '--query SecretString '
                '--output text)'
            ),
            'DB_USER=$(echo "$DB_SECRET" | jq -r .username)',
            'DB_PASSWORD=$(echo "$DB_SECRET" | jq -r .password)',
            'DB_NAME=$(echo "$DB_SECRET" | jq -r .database)',
            (
                'SUPERSET_ADMIN_USER=$('
                'echo "$SUPERSET_ADMIN_SECRET" | jq -r .username)'
            ),
            (
                'SUPERSET_ADMIN_PASSWORD=$('
                'echo "$SUPERSET_ADMIN_SECRET" | jq -r .password)'
            ),
            (
                'SUPERSET_ADMIN_EMAIL=$('
                'echo "$SUPERSET_ADMIN_SECRET" | jq -r .email)'
            ),
            "docker network create analytics-network || true",
            "docker pull postgres:16",
            (
                "docker run -d "
                "--name analytics-postgres "
                "--restart unless-stopped "
                "--network analytics-network "
                "-p 5432:5432 "
                '-e POSTGRES_DB="$DB_NAME" '
                '-e POSTGRES_USER="$DB_USER" '
                '-e POSTGRES_PASSWORD="$DB_PASSWORD" '
                "-v analytics_postgres_data:"
                "/var/lib/postgresql/data "
                "postgres:16"
            ),
            (
                'until docker exec analytics-postgres '
                'pg_isready -U "$DB_USER" -d "$DB_NAME"; '
                "do sleep 5; done"
            ),
            f"docker pull {superset_image}",
            (
                "docker run --rm "
                "--network analytics-network "
                '-e SUPERSET_SECRET_KEY="$SUPERSET_SECRET_KEY" '
                "-v superset_home:/app/superset_home "
                f"{superset_image} "
                "superset db upgrade"
            ),
            (
                "docker run --rm "
                "--network analytics-network "
                '-e SUPERSET_SECRET_KEY="$SUPERSET_SECRET_KEY" '
                "-v superset_home:/app/superset_home "
                f"{superset_image} "
                "superset fab create-admin "
                '--username "$SUPERSET_ADMIN_USER" '
                '--firstname "Cloud" '
                '--lastname "Admin" '
                '--email "$SUPERSET_ADMIN_EMAIL" '
                '--password "$SUPERSET_ADMIN_PASSWORD" '
                "|| true"
            ),
            (
                "docker run --rm "
                "--network analytics-network "
                '-e SUPERSET_SECRET_KEY="$SUPERSET_SECRET_KEY" '
                "-v superset_home:/app/superset_home "
                f"{superset_image} "
                "superset init"
            ),
            (
                "docker run --rm "
                "--network analytics-network "
                '-e HOME="/app/superset_home" '
                "-v superset_home:/app/superset_home "
                f"{superset_image} "
                "pip install --user --no-cache-dir "
                "psycopg2-binary==2.9.12"
            ),
            (
                "docker run --rm "
                "--network analytics-network "
                '-e HOME="/app/superset_home" '
                '-e PYTHONPATH="'
                '/app/superset_home/.local/lib/python3.10/site-packages" '
                "-v superset_home:/app/superset_home "
                f"{superset_image} "
                "python -c "
                "\"import psycopg2; "
                "print('PostgreSQL driver installed successfully')\""
            ),
            (
                "docker run -d "
                "--name apache-superset "
                "--restart unless-stopped "
                "--network analytics-network "
                "-p 8088:8088 "
                '-e SUPERSET_SECRET_KEY="$SUPERSET_SECRET_KEY" '
                '-e HOME="/app/superset_home" '
                '-e PYTHONPATH="'
                '/app/superset_home/.local/lib/python3.10/site-packages" '
                "-v superset_home:/app/superset_home "
                f"{superset_image} "
                "gunicorn "
                "--bind 0.0.0.0:8088 "
                "--workers 1 "
                "--timeout 120 "
                "'superset.app:create_app()'"
            ),
            'echo "Visualization server setup completed."',
        )

        # =============== EC2 instance ===============


        visualization_server = ec2.Instance(
            self,
            "VisualizationServer",
            instance_name="cloud-project-visualization-server",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC,
            ),
            machine_image=(
                ec2.MachineImage.latest_amazon_linux2023()
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO,
            ),
            security_group=server_security_group,
            role=server_role,
            user_data=user_data,
            user_data_causes_replacement=True,
            associate_public_ip_address=True,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=30,
                        volume_type=(
                            ec2.EbsDeviceVolumeType.GP3
                        ),
                        encrypted=True,
                        delete_on_termination=True,
                    ),
                )
            ],
        )


        # =============== Lambda role ===============


        lambda_role = iam.Role(
            self,
            "VisualizationLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/"
                    "AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )

        gold_bucket.grant_read(lambda_role)
        database_secret.grant_read(lambda_role)

        lambda_log_group = logs.LogGroup(
            self,
            "VisualizationLambdaLogGroup",
            log_group_name=(
                "/aws/lambda/cloud-project-gold-to-postgres"
            ),
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )


        # =============== Gold-to-PostgreSQL Lambda ===============

        visualization_function = lambda_.Function(
            self,
            "GoldToPostgres",
            function_name="cloud-project-gold-to-postgres",
            description=(
                "Transfers Gold metrics and KPIs "
                "from S3 into PostgreSQL."
            ),
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                "../app/visualization",
                bundling=cdk.BundlingOptions(
                    image=(
                        lambda_.Runtime.PYTHON_3_12.bundling_image
                    ),
                    command=[
                        "bash",
                        "-c",
                        (
                            "pip install "
                            "-r requirements.txt "
                            "-t /asset-output "
                            "&& cp /asset-input/handler.py "
                            "/asset-output/handler.py"
                        ),
                    ],
                ),
            ),
            role=lambda_role,
            layers=[
                pandas_layer,
            ],
            memory_size=1024,
            ephemeral_storage_size=cdk.Size.mebibytes(1024),
            timeout=Duration.minutes(15),
            retry_attempts=0,
            on_failure=destinations.LambdaDestination(
                notifier_function
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
            ),
            security_groups=[
                lambda_security_group,
            ],
            environment={
                "GOLD_BUCKET": gold_bucket.bucket_name,
                "DB_HOST": (
                    visualization_server.instance_private_ip
                ),
                "DB_PORT": "5432",
                "DB_SECRET_ARN": database_secret.secret_arn,
            },
            log_group=lambda_log_group,
        )

        visualization_function.node.add_dependency(
            visualization_server
        )

        # =============== Schedule ===============

        events.Rule(
            self,
            "DailyVisualizationSyncSchedule",
            description=(
                "Incrementally load new Gold metrics "
                "into PostgreSQL after Gold processing."
            ),
            schedule=events.Schedule.cron(
                minute="30",
                hour="4",
            ),
            targets=[
                targets.LambdaFunction(
                    visualization_function,
                    event=events.RuleTargetInput.from_object(
                        {
                            "mode": "incremental",
                        }
                    ),
                    retry_attempts=2,
                )
            ],
        )

        # =============== Outputs ===============

        cdk.CfnOutput(
            self,
            "VisualizationServerInstanceId",
            value=visualization_server.instance_id,
        )

        cdk.CfnOutput(
            self,
            "VisualizationServerPublicIp",
            value=visualization_server.instance_public_ip,
        )

        cdk.CfnOutput(
            self,
            "SupersetUrl",
            value=cdk.Fn.join(
                "",
                [
                    "http://",
                    visualization_server.instance_public_ip,
                    ":8088",
                ],
            ),
        )

        cdk.CfnOutput(
            self,
            "VisualizationLambdaName",
            value=visualization_function.function_name,
        )

        cdk.CfnOutput(
            self,
            "DatabaseSecretArn",
            value=database_secret.secret_arn,
        )

        cdk.CfnOutput(
            self,
            "SupersetAdminSecretArn",
            value=superset_admin_secret.secret_arn,
        )