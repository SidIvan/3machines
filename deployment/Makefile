run1:
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	rm /var/log/writer/.writer.log.swp
	docker compose -f docker-compose1.yaml build --no-cache
	docker compose -f docker-compose1.yaml up -d --force-recreate

stop1:
	docker compose -f docker-compose1.yaml stop

run2:
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	rm /var/log/writer/.writer.log.swp
	docker compose -f docker-compose2.yaml build --no-cache
	docker compose -f docker-compose2.yaml up -d --force-recreate

stop2:
	docker compose -f docker-compose1.yaml stop

clearDocker:
	docker system prune -a