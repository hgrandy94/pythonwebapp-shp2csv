# Install system packages and updates
sudo apt update
sudo apt install build-essential

# Install pyodbc dependencies
sudo apt install unixodbc
sudo apt install unixodbc-dev

# Install ODBC driver 13 for SQL
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install msodbcsql
# optional: for bcp and sqlcmd
sudo ACCEPT_EULA=Y apt-get install mssql-tools
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
source ~/.bashrc

# Download and install mini conda
wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh -O ~/miniconda.sh

bash ~/miniconda.sh -b -f -p $HOME/miniconda

# Add miniconda to path
echo 'export PATH="$HOME/miniconda/bin:$PATH"' >> ~/.bash_profile
echo 'export PATH="$HOME/miniconda/bin:$PATH"' >> ~/.bashrc


# Run pip install cmds for ADLS and sql
pip install azure-mgmt-resource
pip install azure-mgmt-datalake-store
pip install azure-datalake-store
pip install pyodbc

# Install the GDAL library
conda install gdal
