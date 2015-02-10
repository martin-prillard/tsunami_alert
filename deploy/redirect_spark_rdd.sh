# redirect spark rdd on other partition
sudo mkdir /raid0/spark/
sudo mv /var/lib/spark/* /raid0/spark/
sudo rmdir /var/lib/spark/
sudo ln -s /raid0/spark/ /var/lib/