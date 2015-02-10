# install anaconda
cd ~
wget http://09c8d0b2229f813c1b93-c95ac804525aac4b6dba79b00b39d1d3.r79.cf1.rackcdn.com/Anaconda-1.9.2-Linux-x86_64.sh
bash Anaconda-1.9.2-Linux-x86_64.sh -b
cd ~
echo "export PATH=/home/ubuntu/anaconda/bin:$PATH" >> .bashrc
export PATH=/home/ubuntu/anaconda/bin:$PATH
sudo apt-get install libev4 libev-dev

# install modules
cd ~
pip install cassandra-driver
pip install geopy
pip install cqlengine
sudo apt-get install redis-server

# launch redis-server
redis-server &

# git clone tsunami project
cd ~
git clone https://github.com/martin-prillard/tsunami_alert.git
