from aws_cdk import (
    Duration,
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda as lambda_
)
from constructs import Construct

class TestStateMachineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # -------------------------------------------------------------------
        # Define Lambdas for the Contractual Package processing (global)
        # -------------------------------------------------------------------
        lambda_HeaderPreprocessing = lambda_.Function(
            self, "HeaderPreprocessing",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.HeaderPreprocessing",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )
    
        lambda_TableExtraction = lambda_.Function(
            self, "TableExtraction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.TableExtraction",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        lambda_FeatureExtraction = lambda_.Function(
            self, "FeatureExtraction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.FeatureExtraction",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        lambda_HeaderExtraction = lambda_.Function(
            self, "HeaderExtraction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.HeaderExtraction",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        lambda_CPAggr = lambda_.Function(
            self, "CPAggr",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.CPAggr",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        # -------------------------------------------------------------------
        # Define tasks for the Contractual Package processing (global)
        # -------------------------------------------------------------------
        # Task: Invoke HeaderPreprocessing Lambda
        task_HeaderPreprocessing = tasks.LambdaInvoke(
            self, "InvokeHeaderPreprocessing",
            lambda_function=lambda_HeaderPreprocessing,
            output_path="$.Payload"
        )

        # Task: Invoke HeaderExtraction Lambda (uses output from HeaderPreprocessing)
        task_HeaderExtraction = tasks.LambdaInvoke(
            self, "InvokeHeaderExtraction",
            lambda_function=lambda_HeaderExtraction,
            output_path="$.Payload"
        )

        # Chain: Process header tasks sequentially (HeaderPreprocessing then HeaderExtraction)
        branch1 = sfn.Chain.start(task_HeaderPreprocessing).next(task_HeaderExtraction)

        # Task: Invoke TableExtraction Lambda
        task_TableExtraction = tasks.LambdaInvoke(
            self, "InvokeTableExtraction",
            lambda_function=lambda_TableExtraction,
            output_path="$.Payload"
        )

        # Task: Invoke FeatureExtraction Lambda
        task_FeatureExtraction = tasks.LambdaInvoke(
            self, "InvokeFeatureExtraction",
            lambda_function=lambda_FeatureExtraction,
            output_path="$.Payload"
        )

        # Task: Invoke CPAggr Lambda
        task_CPAggr = tasks.LambdaInvoke( 
            self, "InvokeCPAggr",
            lambda_function=lambda_CPAggr,
            output_path="$.Payload"
        )

        # Create a Parallel state that runs header processing (branch1), table extraction, and feature extraction in parallel.
        # When all parallel branches complete, it continues with the CPAggr task.
        contractual_chain = (
            sfn.Parallel(self, "ParallelSubtasks")
            .branch(branch1)
            .branch(task_TableExtraction)
            .branch(task_FeatureExtraction)
        ).next(task_CPAggr)

        # Create the Contractual Package State Machine from the contractual_chain.
        # This state machine is invoked later for each contractual package.
        contractual_sm = sfn.StateMachine(
            self, "ContractualPackageStateMachine",
            state_machine_name="ContractualPackageStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(contractual_chain),
        )

        # -------------------------------------------------------------------
        # Define Lambdas for PDF processing (global)
        # -------------------------------------------------------------------
        lambda_GeneralPreprocessing = lambda_.Function(
            self, "GeneralPreprocessing",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.GeneralPreprocessing",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        lambda_SplitInd = lambda_.Function(
            self, "SplitInd",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.SplitInd",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        lambda_Div = lambda_.Function(
            self, "Div",
            runtime=lambda_.Runtime.PYTHON_3_8,
            handler="functions.Div",
            code=lambda_.Code.from_asset("test_state_machine/functions")
        )

        # -------------------------------------------------------------------
        # Function to create a new processing chain for each PDF
        # -------------------------------------------------------------------
        def create_pdf_processing_chain() -> sfn.Chain:
            # Create new instances of tasks for processing a PDF.
            task_gp = tasks.LambdaInvoke(
                self, "InvokeGeneralPreprocessing_Task",
                lambda_function=lambda_GeneralPreprocessing,
                output_path="$.Payload"
            )
            task_si = tasks.LambdaInvoke(
                self, "InvokeSplitInd_Task",
                lambda_function=lambda_SplitInd,
                output_path="$.Payload"
            )
            task_div = tasks.LambdaInvoke(
                self, "InvokeDiv_Task",
                lambda_function=lambda_Div,
                output_path="$.Payload"
            )
            # Chain the PDF processing tasks sequentially.
            pdf_chain_local = sfn.Chain.start(task_gp).next(task_si).next(task_div)

            # Create a Map state to process the contractual packages returned by Lambda Div.
            # It expects the Div function to return an object with a "contractualPackages" array.
            contractual_map_local = sfn.Map(
                self, "ProcessContractualPackages",
                items_path="$.contractualPackages",  # Path to the array in the Div output.
                # max_concurrency can be specified as a string if needed, e.g., max_concurrency="300"
                result_path="$.contractualPackageResults"
            )
            # Define the task that starts the Contractual Package state machine for each package.
            contractual_pkg_task_local = tasks.StepFunctionsStartExecution(
                self, "StartContractualPackageExecution_Task",
                state_machine=contractual_sm,
                integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                input=sfn.TaskInput.from_json_path_at("$")
            )
            # Use the new API "item_processor" (instead of the deprecated "iterator")
            contractual_map_local.item_processor(contractual_pkg_task_local)

            # Append the contractual packages processing to the PDF processing chain.
            overall_chain_local = pdf_chain_local.next(contractual_map_local)
            return overall_chain_local

        # -------------------------------------------------------------------
        # Create a Map state to process multiple PDFs in parallel.
        # The global input is expected to have a "pdfs" property (an array of PDF objects).
        # -------------------------------------------------------------------
        pdf_map = sfn.Map(
            self,
            "ProcessPDFs",
            items_path="$.pdfs",       # Each element contains PDF information (e.g., S3 bucket, key, etc.)
            # You can specify max_concurrency if needed, e.g., max_concurrency="100",
            result_path="$.pdfResults"
        )
        # For each PDF, generate a new processing chain.
        pdf_map.item_processor(create_pdf_processing_chain())

        # -------------------------------------------------------------------
        # Create the global State Machine that processes all PDFs.
        # -------------------------------------------------------------------
        sfn.StateMachine(
            self, "PDFStateMachine",
            state_machine_name="PDFStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(pdf_map),
        )
