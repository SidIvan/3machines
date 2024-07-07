run: clearLog
	docker compose -f docker-compose.yaml up -d --force-recreate --build sizif

run: stop
	docker compose -f docker-compose.yaml up down sizif

runAll: clearLog
	docker compose -f docker-compose.yaml up -d --force-recreate --build

restartAll: stopAll
	docker compose -f docker-compose.yaml up -d

stopAll:
	docker compose -f docker-compose.yaml stop


clearLog:
	rm /var/log/sizif/sizif.log
	touch /var/log/sizif/sizif.log
	rm /var/log/sizif/.sizif.log.swp || true

clearDocker:
	docker system prune -a