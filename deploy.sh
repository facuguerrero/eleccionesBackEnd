#!/bin/bash

echo "Cloning GitHub repository..."
git clone git@github.com:facuguerrero/eleccionesBackEnd.git
echo "Copying credentials..."
cp ./twitter_credentials.json ./eleccionesBackEnd/
cd eleccionesBackEnd/
echo "Removing unneeded files..."
rm -rf .git
rm .gitignore
rm README.md
cd ..
echo "Compressing..."
tar -czf backend.tar.gz eleccionesBackEnd/
echo "Sending .tar.gz via scp..."
scp ./backend.tar.gz elecciones@opladyn.fi.uba.ar:~/
echo "Deleting .tar.gz..."
rm ./backend.tar.gz
echo "Deleting repository folder..."
rm -rf ./eleccionesBackEnd/
echo "Connecting to server via SSH..."
ssh elecciones@opladyn.fi.uba.ar
echo "Deployment finished."
