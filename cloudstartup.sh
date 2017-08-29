#! /bin/bash
sudo passwd
sudo passwd $USER

sudo apt-get update && sudo apt-get dist-upgrade -y
sudo apt-get install zsh stow glances -y

ssh-keygen -b 2048 -t rsa
cat .ssh/id_rsa.pub


# clone
git clone https://github.com/powerline/fonts.git
# install
cd fonts
./install.sh
# clean-up a bit
cd ..
rm -rf fonts

chsh -s /bin/zsh
sudo apt-get -y install stow

cd ~
#git clone git@github.com:tarjoilija/zgen.git
git clone https://github.com/tarjoilija/zgen.git
#git clone git@github.com:unixorn/zsh-quickstart-kit.git
git clone https://github.com/unixorn/zsh-quickstart-kit.git

cd zsh-quickstart-kit
stow --target=/home/$USER zsh
sudo apt-get install nodejs-legacy -y

sleep 5
source .zshrc

zsh




curl -L https://raw.githubusercontent.com/pyenv/pyenv-installer/master/bin/pyenv-installer | zsh
git clone https://github.com/pyenv/pyenv-virtualenv.git $(pyenv root)/plugins/pyenv-virtualenv

echo PATH="/home/jonathanpoczatek/.pyenv/bin:$PATH" >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.zshrc
exec $SHELL

pyenv update
