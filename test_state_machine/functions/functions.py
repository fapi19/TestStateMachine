import time

def HeaderPreprocessing(event, context):
    time.sleep(2)
    return {"status": "HeaderPreprocessing complete"}

def TableExtraction(event, context):
    time.sleep(2)
    return {"status": "TableExtraction complete"}

def FeatureExtraction(event, context):
    time.sleep(2)
    return {"status": "FeatureExtraction complete"}

def HeaderExtraction(event, context):
    time.sleep(2)
    return {"status": "HeaderExtraction complete"}

def CPAggr(event, context):
    time.sleep(2)
    return {"status": "CPAggr complete"}

def GeneralPreprocessing(event, context):
    time.sleep(2)
    
    package_id = event.get("packageId", "")
    processed_package = {"packageId": package_id + "_proc"}
    return processed_package

def SplitInd(event, context):
    time.sleep(2)
    return {"status": "SplitInd complete"}

def Div(event, context):
    time.sleep(2)
    package_id = event.get("processedData", "")
    
    sub_contractualPackages = [
        {"packageId": f"sub_{package_id}_1"},
        {"packageId": f"sub_{package_id}_2"}
    ]
    
    return {
        "contractualPackages": package_id,  
        "sub_contractualPackages": sub_contractualPackages
    }
