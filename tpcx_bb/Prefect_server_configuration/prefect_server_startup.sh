prefect backend server
prefect server start >> prefect_server.log 2>&1 &
sleep 10
prefect agent start --name "rapids-flows-agent" > prefect_agent.log 2>&1 &
sleep 10
prefect create project "Tpcx-bb-rapids"

