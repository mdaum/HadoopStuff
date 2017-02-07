(1) ssh from a department server (e.g., classroom or compute) to the cluster login node using the preferred login you gave me.  Here is the command I use:
  ssh smithfd@152.2.23.173
The password is TopsailCluster  (note the two capitals)
Once you are logged in, immediately use the 'passwd' command to change to a personal password chosen by you.  If you cannot login, please send me email.

(2) in your home directory on the cluster machine, create a directory to hold the example MapReduce code we will be discussing Wednesday.

(3) in your CS home directory, create a directory to hold the example MapReduce code.

(4) On a CS server, use the cp command to copy the example files from a public directory  under my login.  Here is an example (assuming your current directory is the destination).

  cp /home/smithfd/public/790-042-s17/MR-example/* .

You should have 3 .java files and 2 .sh files

(5) On the cluster login node, use the scp command and your CS password to copy the example files from your CS directory to the destination on the cluster node.  Here is an example (assuming your current directory there is the destination).  Do the same for the .sh files.

  scp -r <your CS login>@compute.cs.unc.edu:/home/<your CS login>/<path to directory>/*.java .