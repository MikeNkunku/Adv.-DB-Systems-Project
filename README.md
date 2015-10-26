# Advanced DB Systems Project

## Gettin started

### Download Git (for Windows users)
You need to download the matching version for your OS over [here](https://git-scm.com/downloads).
You will then need to add git to your system environment variables in order to use from the command line.

### Forking the project
After logging in and being on this page, you have to fork the project so as to have it displayed on your own profile.

### Cloning the project locally
Go to your profile, then in the project you just forked. You will see on the right a link labelled "HTTPS clone URL".
Copy the link (I will call it "linked_url") and tap the following command in a terminal/command line:
```
git clone linked_url
```

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