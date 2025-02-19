import boto3
import json
import datetime
def invoke_state_machine():
    # Create a boto3 session using your specified profile.
    session = boto3.Session(profile_name='Proactive')
    sf_client = session.client('stepfunctions', region_name='us-west-2')
    
    # Define a sample input payload.
    # In this case, we include a "pdfs" key with an array of dummy PDF data.
    num_pdfs = 20
    input_payload = {
        "pdfs": [{"packageId": f"data{i+1}"} for i in range(num_pdfs)]
    }

    print(input_payload)

    # Replace with the ARN of your state machine.
    state_machine_arn = "arn:aws:states:us-west-2:843633125939:stateMachine:PDFStateMachine"
    execution_name = f"testing_in_code_01_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    try:
        response = sf_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_payload)
        )
        print("Started execution")
        print(response.get('executionArn'))
    except Exception as e:
        print("Error starting execution")
        print(str(e))

if __name__ == '__main__':
    invoke_state_machine()
