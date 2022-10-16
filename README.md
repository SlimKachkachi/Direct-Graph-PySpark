# Master-IA-Big-Data-Dauphine-Spark
Projets sur Spark Rdd & DF
Tous les jeux de données sont issus de SNAP Stanford

fichier CCF_DF_Soc_protec_googlecloud_dataproc.py => version code DataFrame pour spark DataProc (jeu de données Soc-pokec-relationships)
fichier CCF_RDD_Soc_protec_googlecloud_dataproc.py => version code RDD pour spark DataProc (m^me jeu de données)
fichier CFF_RDD_jupyter => version sur Jupyter Notebook + Spark local sur MacBook Pro
fichier CCF_RDD_*.py => version RDD 6 jeux de données issus de SNAP Stanford avec 2 codes (dont un pour contrôle) sous Databricks
fichier CCF_DF_*.py => version RDD 6 jeux de données issus de SNAP Stanford avec 2 codes (dont un pour contrôle) sous Databricks

pour mémoire principales commandes HDFS Cluster Spark
  > wget https://www.dropbox.com/xxx => pour récupérer fichier à partir de dropbox par exemple
  > chmod +x *.py => ne pas oublier de donner les droits en exécution sur le fichier x.py
  > hdfs dfs -mkdir /user/xx => pour créer un répertoir pour y déposer les fichiers de données
  > hdfs dfs -put shake.txt /user/xx/.  => pour y déposer le fichier de données (attention au /.)
  > hdfs fsck /user/xx/nom_fichier_data.xx -files -blocks -locations => pour vérifier que la partition HDFS du jeux de données est active
  > hdfs dfs -ls /user/xx  => pour lister le contenu 
  > les commandes unix de base fonction comme rm/cp... 
  > spark-submit nom_fichier.py. => pour lancer le job
 
