# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.

import logging
import json
import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    website=json.loads(context.get_input())["website"]
    result = yield context.call_activity("c"+website, context.get_input())

    return result

main = df.Orchestrator.create(orchestrator_function)
