https://medium.com/@lubomir-tobek/kubernetes-multi-master-node-cluster-f2081e504983 - статья



{
  sudo mkdir -p /etc/etcd /var/lib/etcd /var/lib/kubernetes/pki
  sudo cp etcd-server.key etcd-server.crt /etc/etcd/
  sudo cp /etc/ssl/certs/ca-certificates.crt /var/lib/kubernetes/pki/
  sudo chown root:root /etc/etcd/*
  sudo chmod 600 /etc/etcd/*
  sudo chown root:root /var/lib/kubernetes/pki/*
  sudo chmod 600 /var/lib/kubernetes/pki/*
  sudo ln -s /var/lib/kubernetes/pki/ca-certificates.crt /etc/etcd/ca.crt
}