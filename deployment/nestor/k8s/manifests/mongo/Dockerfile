FROM mongo:6.0.20 as build
COPY deployment/nestor/k8s/manifests/mongo/init.sh /init.sh
COPY deployment/nestor/k8s/manifests/mongo/entrypoint.sh /entrypoint.sh
RUN chmod +x /init.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]