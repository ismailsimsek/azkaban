# Azkaban 

This is source code of azkaban application we are using in BI, our main workflow and scheduling tool. 
this repository includew following minor patches we did. Master azkaban repository is here https://github.com/azkaban/azkaban

PPRO changes
you can see changes by comparing 'ppropatches' branch with master

# Changes
## XmlUserManager2
it enables us to add hashed passwords to usermanager file. 
## XmlMD5UserManager
it enables us to add MD5 hashed passwords to usermanager file. not used at the moment. 

## FLOW_EXECUTION_DIR
adds new runtime parametter (${azkaban.flow.execution.dir}) which is giving "Root directory of the execution."

