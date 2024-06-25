
run_tests:
	docker build \
		--platform linux/amd64 \
		-t damage-assessment . && \
	docker run -it \
		-v $(PWD):/app \
		--env-file .env \
		--entrypoint python3 damage-assessment -m pytest
