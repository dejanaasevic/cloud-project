#!/usr/bin/env python3

import aws_cdk as cdk

from stacks.infrastructure_stack import InfrastructureStack
from stacks.visualization_stack import VisualizationStack


app = cdk.App()

infrastructure_stack = InfrastructureStack(
    app,
    "InfrastructureStack",
)

visualization_stack = VisualizationStack(
    app,
    "VisualizationStack",
    vpc=infrastructure_stack.vpc,
    gold_bucket=infrastructure_stack.gold_bucket,
    pandas_layer=infrastructure_stack.pandas_layer,
    notifier_function=infrastructure_stack.notifier_fn,
)

visualization_stack.add_dependency(
    infrastructure_stack
)

app.synth()