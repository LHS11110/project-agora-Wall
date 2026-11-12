defmodule AgoraBroker.CanvasService do
  # Fixed internal destination, TLS verification and a route-bound identity.
  def query(input) do
    identity=input["identity"]
    payload=Jason.encode!(%{"canvasId"=>input["route"]["canvas_id"],"userId"=>identity["user_id"],"clientIp"=>identity["client_ip"],"request"=>input["event"]["service"]})
    token=Application.fetch_env!(:agora_broker,:internal_token)
    options=%{transport: :tls, protocols: [:http], tls_opts: [verify: :verify_peer, versions: [:"tlsv1.3"], cacertfile: ~c"/run/tls/ca.pem", server_name_indication: ~c"agora-nginx", customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]]}
    with {:ok,conn} <- :gun.open(~c"agora-nginx",8444,options) do
      try do
        with {:ok,_} <- :gun.await_up(conn,5000),
             ref <- :gun.post(conn,"/internal/canvas/socket",[{"content-type","application/json"},{"x-agora-internal-token",token}],payload),
             {:response,fin,status,_} <- :gun.await(conn,ref,60_000),
             {:ok,body} <- if(fin==:fin,do: {:ok,"{}"},else: :gun.await_body(conn,ref,60_000)),
             true <- byte_size(body)<=2_097_152,
             {:ok,data} <- Jason.decode(body) do
          response=if status in 200..299,do: %{"type"=>"runtime_result","ok"=>true,"result"=>data},else: %{"type"=>"runtime_result","ok"=>false,"status"=>status,"code"=>data["code"]||"CANVAS_SERVICE_REJECTED","message"=>data["message"]||data["detail"]||data["error"]||"캔버스 서비스 요청이 거부되었습니다."}
          {:ok,%{"route"=>input["route"],"response"=>response}}
        else
          _ -> {:error,"CANVAS_SERVICE_UNAVAILABLE"}
        end
      after
        :gun.close(conn)
      end
    else
      _ -> {:error,"CANVAS_SERVICE_UNAVAILABLE"}
    end
  rescue
    _ -> {:error,"CANVAS_SERVICE_UNAVAILABLE"}
  catch
    _,_ -> {:error,"CANVAS_SERVICE_UNAVAILABLE"}
  end
end
