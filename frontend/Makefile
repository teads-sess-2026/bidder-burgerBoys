build_docker:
	docker build -t sess-frontend .

run_docker:
	docker run -d -p 80:80 sess-frontend

stop_docker:
	docker ps -q --filter ancestor="sess-frontend" | xargs -r docker stop
