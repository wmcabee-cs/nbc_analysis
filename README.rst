============
nbc_analysis
============


Stress testing and data analysis for NBC poc




============
Installation
============


---------------------
ec2 instance setup
---------------------
obtain following items from solutions architect (Rose)

    * Console:  User name & password
    * for connection:  Access key ID, Secret access key

Then follow instructions for creating ~/.aws/credentials and ~/.aws/config

### prerequisites
# Setup pyenv using steps outlined at https://gist.github.com/mrthomaskim/e48d747816cfac0b481684e7a5084e48

sudo yum groupinstall "Development Tools"
git --version
gcc --version
bash --version
python --version # (system)
sudo yum install -y openssl-devel readline-devel zlib-devel
sudo yum update

### install `pyenv`
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bash_profile
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bash_profile
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.bash_profile

### restart SHELL and install `python`
echo $PATH
echo $(pyenv root)
pyenv install --list
pyenv install 3.7.4
pyenv versions

### install `pyenv virtualenv`
git clone https://github.com/pyenv/pyenv-virtualenv.git $(pyenv root)/plugins/pyenv-virtualenv
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bash_profile

### restart SHELL and virtualenv
pyenv virtualenv 3.6.3 test-3.6.3
pyenv virtualenvs
pyenv local test-3.6.3
pyenv version
cat .python-version #> test-3.6.3

pyenv rehash

python --version #> Python 3.6.3
pip list --format=columns


