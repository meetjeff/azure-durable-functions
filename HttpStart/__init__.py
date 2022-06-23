# This function an HTTP starter function for Durable Functions.
 
import logging
import azure.functions as func
import azure.durable_functions as df
import json 

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    
    w = req.params.get('website')
    if w not in ['yourator','104','518','1111']:
        return func.HttpResponse('請選擇以下平台:yourator、104、518、1111')


    s = req.params.get('search')
    if not s:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            s = req_body.get('search')

    if s:
        sp=s.replace('+','、').replace(' ','、')
        
    else:
        return func.HttpResponse(
             "請輸入查詢關鍵字",
             status_code=200
        )


    inp = req.params.get('page')
    if not inp:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            inp = req_body.get('page')
    
    if inp:
        try:
            rp = int(inp)
        except:
            return func.HttpResponse('請輸入正確頁數')
    
    else:
        rp = 10


    data={"website":w,"search":sp,"page":rp}
    jdata=json.dumps(data,ensure_ascii=False)

    instance_id = await client.start_new(req.route_params["functionName"], None, jdata)

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return client.create_check_status_response(req, instance_id)
