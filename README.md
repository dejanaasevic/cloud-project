# Hacker News & Twitter Data Pipeline

A serverless data platform on AWS that ingests daily data from **Hacker News** and **Twitter (X)**, processes it through a medallion (Bronze → Silver → Gold) architecture, and syncs the resulting KPIs into a PostgreSQL database visualized with **Apache Superset**.

Built with **AWS CDK (Python)**. All ingestion and processing runs on scheduled Lambda functions — there are no always-on servers except the small EC2 instance hosting Postgres/Superset for the dashboards.

## Architecture

Data flows through a medallion pipeline: Bronze (raw HN + Twitter JSON) → Silver (normalized Parquet) → Gold (daily KPIs/metrics) → PostgreSQL (incremental sync) → Apache Superset dashboards. Any Lambda failure is routed through an EventBridge destination to a Discord webhook notification.

Every layer is idempotent: each Lambda checks whether its output for the target date already exists before doing any work, so retries and re-runs are safe.

### Bronze layer
- **Hacker News** (`app/bronze/hacker_news`) — pulls yesterday's stories, comments, asks, jobs and polls from the [HN Algolia Search API](https://hn.algolia.com/api), plus author profiles from the Firebase API, and writes them as raw JSON, partitioned by date.
- **Twitter** (`app/bronze/twitter`) — streams the [COVID-19 tweets Kaggle dataset](https://www.kaggle.com/datasets/gpreda/covid19-tweets) in chunks and extracts tweets matching a target day (since the dataset itself is historical, a random day is sampled as a stand-in for "yesterday").

### Silver layer
- Normalizes both sources (`app/silver/hacker_news`, `app/silver/twitter`) into a common `users` / `posts` schema and writes partitioned Parquet via [AWS SDK for pandas](https://aws-sdk-pandas.readthedocs.io/) (`awswrangler`). Runs isolated in a VPC private subnet with S3-only network access.

### Gold layer
- `app/gold` computes daily KPIs across both platforms: post counts by type, active users per platform, top users by karma/followers, top posts by score, and a data-quality score based on non-null field coverage.

### Visualization
- `app/visualization` reads Gold Parquet tables and incrementally UPSERTs them into PostgreSQL (tracking a watermark per table so re-runs only pick up new/updated partitions).
- PostgreSQL and Apache Superset run in Docker on a single EC2 instance, provisioned entirely through user-data (see `infrastructure/stacks/visualization_stack.py`).

### Notifications
- `app/notifications/discord_notifier` receives Lambda async-failure payloads (wired up as an `on_failure` destination on every pipeline function) and posts a formatted alert to a Discord webhook.

## Infrastructure

Defined with AWS CDK in [`infrastructure/`](infrastructure/):

- [`InfrastructureStack`](infrastructure/stacks/infrastructure_stack.py) — VPC, S3 buckets (Bronze/Silver/Gold), all pipeline Lambdas, EventBridge schedules, and the Discord notifier.
- [`VisualizationStack`](infrastructure/stacks/visualization_stack.py) — Secrets Manager credentials, the Postgres/Superset EC2 instance, the Gold-to-Postgres Lambda, and its schedule.

Key design points:
- Lambdas that only need to reach S3 run in an isolated private subnet with a security group scoped to the S3 gateway endpoint prefix list — no NAT gateway required.
- S3 buckets use `RemovalPolicy.RETAIN` and are looked up if they already exist, so redeploying a deleted stack doesn't collide with orphaned buckets from a previous deploy.
- Daily EventBridge schedules run the pipeline in order: Bronze → Silver → Gold → Postgres sync (01:00–04:30 UTC).

## Prerequisites

- Python 3.12+
- AWS account + credentials configured (`aws configure`)
- [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) (`npm install -g aws-cdk`)
- A [Kaggle](https://www.kaggle.com/) account/API key (for the Twitter bronze ingestion)
- A Discord webhook URL (for failure notifications)

## Setup & Deployment

```bash
cd infrastructure
python -m venv .venv
.venv\Scripts\activate.bat      # Windows
# source .venv/bin/activate     # macOS/Linux

pip install -r requirements.txt
```

Set the environment variables the stacks read at synth/deploy time:

```bash
set KAGGLE_USERNAME=your-kaggle-username
set KAGGLE_KEY=your-kaggle-key
set DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

Bootstrap (first time only) and deploy:

```bash
cdk bootstrap
cdk deploy --all --parameters VisualizationStack:SupersetAllowedCidr=<your-public-ip>/32
```

`SupersetAllowedCidr` restricts access to the Superset UI (port 8088) to a single IPv4 CIDR — use your own public IP. Postgres (port 5432) is only reachable from the visualization Lambda's security group.

Once deployed, the `SupersetUrl` stack output gives you the dashboard address, and `SupersetAdminSecretArn` / `DatabaseSecretArn` point to the generated credentials in Secrets Manager.

## Useful CDK commands

```bash
cdk ls        # list all stacks
cdk synth     # emit the synthesized CloudFormation template
cdk diff      # compare deployed stack with current state
cdk destroy   # tear down (S3 buckets are retained)
```

## Tests

```bash
cd infrastructure
pip install -r requirements-dev.txt
pytest
```
