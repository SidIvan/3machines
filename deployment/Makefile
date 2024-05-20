run: clearLog=
	docker compose -f docker-compose.yaml up -d --force-recreate --build

restartAll: stopAll
	docker compose -f docker-compose.yaml up -d

stopAll:
	docker compose -f docker-compose.yaml stop


clearLog:
	rm /var/log/writer/writer.log
	touch /var/log/writer/writer.log
	rm /var/log/writer/.writer.log.swp || true

clearDocker:
	docker system prune -a