.PHONY: bootstrap  prepare-data run-local build destroy-infra-run local-infra-run deploy-local-infra deploy-prod

bootstrap:
	brew tap hashicorp/tap
	brew install hashicorp/tap/terraform
	brew install awscli

prepare-data:
	mkdir -p data-integration/data/github/issues/`date +%Y-%m-%d`/
	mkdir -p data-integration/data/github/pull_requests/`date +%Y-%m-%d`/
	unzip  data/prepared_issues.json.zip -d data-integration/data/github/issues/`date +%Y-%m-%d`/
	unzip data/prepared_pull_requests.json.zip  -d data-integration/data/github/pull_requests/`date +%Y-%m-%d`/

run-local:
	docker build . --tag data-ingest:latest
	docker run --rm --name data-ingest  data-ingest:latest  --job_name sta_github_issues  --app_name github

package:
	$(MAKE) -C ./data-integration package

destroy-local-infra:
	docker compose -f docker-compose.yml down
	docker compose -f docker-compose-spark.yml down

deploy-local-infra:
	docker compose -f docker-compose.yml up -d

deploy-spark:
	docker compose -f docker-compose-spark.yml up -d

deploy-job:
	$(MAKE) -C ./data-integration deploy-job

deploy-prod: destroy-local-infra build
	$(MAKE) -C ./aws-terraform terraform-plan



