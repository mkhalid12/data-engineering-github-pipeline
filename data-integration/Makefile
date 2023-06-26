package:
	rm -rf ./dist && mkdir ./dist
	cp main.py ./dist
	cp config_prod.json ./dist/config.json
	zip -r ./dist/jobs.zip jobs
	zip -r ./dist/lib.zip lib

deploy-job:
	docker-compose exec -w /opt/bitnami/spark/dist spark spark-submit \
	--master spark://host.docker.internal:7077 \
	--packages org.postgresql:postgresql:42.6.0 \
	--py-files jobs.zip,lib.zip \
	--files config.json \
	main.py --job $(job_name) --app $(job_name)
