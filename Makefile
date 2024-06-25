
run_tests:
	docker build \
		--platform linux/amd64 \
		-t damage-assessment . && \
	docker run -it \
		-v $(PWD):/app \
		--env-file .env \
		--entrypoint python3 damage-assessment -m pytest

run_tests_ci:
	docker build \
		--platform linux/amd64 \
		-t damage-assessment . && \
	docker run \
		-v $(PWD):/app \
		--env GS_SECRET_ACCESS_KEY=${GS_SECRET_ACCESS_KEY} \
		--env GS_ACCESS_KEY_ID=${GS_ACCESS_KEY_ID} \
		--entrypoint python3 damage-assessment -m pytest
