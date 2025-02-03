import aws_cdk as core
import aws_cdk.assertions as assertions

from test_state_machine.test_state_machine_stack import TestStateMachineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in test_state_machine/test_state_machine_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = TestStateMachineStack(app, "test-state-machine")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
