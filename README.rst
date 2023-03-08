=========
pvaServer
=========

pvaServer CLI to stream image data to PVA

Usage
=====

::

	pvaserver -h
	usage: pvaserver [-h] [--config FILE] [--version]  ...

	optional arguments:
	  -h, --help     show this help message and exit
	  --config FILE  File name of configuration file
	  --version      show program's version number and exit

	Commands:
	  
	    init         Usage: pvaserver init - Create configuration file and restore the original default values
	    sim          Usage: pvaserver sim - Run the PVA server in simulation mode (-h for more options)
	    stack        Usage: pvaserver stack --file-path /data/ - Run the PVA server loading a stack of images from a folder (-h for more options)
	    tomo         Usage: pvaserver tomo --file-name tomo.h5 - Run the PVA server loading a tomo dataset (-h for more options)
	    status       Usage: pvaserver status - Show status


