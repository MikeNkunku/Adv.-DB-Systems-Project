# Advanced DB Systems Project

## How to start

### Download Git (for Windows users)
You need to download the matching version for your OS over [here](https://git-scm.com/downloads).
You will then need to add git to your system environment variables in order to use from the command line.

### Clone the project locally
In a terminal/command line, after in the folder you chose, you have to tap the following line:
	git clone https://github.com/MikeNkunku/Adv.-DB-Systems-Project.git

## Using Git

### Help
* In order to know the name of all Git commands, tap:
```
git help
	```

* To know how to use a Git command, tap:
```
git <Git_command_name> help
```

To resolve conflicts, just follow what Git displays on command line.

### Display the list of modified files and the local status compared to the one on Git
	git status

### Before starting working
In order to have the latest version of the project, you must retrieve from the Git server by tapping the following command on the terminal:
```
git pull --verbose
```

### Committing your changes
	git add <fileName (including its extension)>
	git commit -m "<message to easily identify changes which were made afterwards>"

### Save your changes on Git
	git push

You will then be asked to enter your username, and then password.