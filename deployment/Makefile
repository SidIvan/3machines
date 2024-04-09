run1:
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	docker compose -f docker-compose1.yaml build --no-cache
	docker compose -f docker-compose1.yaml up -d --force-recreate

run2:
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	docker compose -f docker-compose2.yaml build --no-cache
	docker compose -f docker-compose2.yaml up -d --force-recreate

clearDocker:
	docker system prune -a