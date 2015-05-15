#! /bin/sh

mvn clean package
hadoop fs -mkdir /xmlextraction
hadoop fs -mkdir /xmlextraction/stores
hadoop fs -rm -skipTrash /xmlextraction/inventory/part-r-00000
hadoop fs -rm -skipTrash /xmlextraction/inventory/_SUCCESS
hadoop fs -rmdir /xmlextraction/inventory
hadoop fs -rm -skipTrash /xmlextraction/book/part-r-00000
hadoop fs -rm -skipTrash /xmlextraction/book/_SUCCESS
hadoop fs -rmdir /xmlextraction/book


hadoop fs -copyFromLocal ../testdata/InventorySoho.xml /xmlextraction/stores
hadoop fs -copyFromLocal ../testdata/InventoryBanks.xml /xmlextraction/stores
hadoop fs -ls /xmlextraction/stores
hadoop fs -cat /xmlextraction/stores/InventorySoho.xml
hadoop fs -cat /xmlextraction/stores/InventoryBanks.xml

hadoop jar ../target/HadoopXmlExtractor-1.0-SNAPSHOT.jar HadoopXmlExtractor.CreateSequenceFile /xmlextraction/stores/ /xmlextraction/seqfiles/inventory.seq
hadoop fs -ls /xmlextraction/seqfiles

hadoop jar ../target/HadoopXmlExtractor-1.0-SNAPSHOT.jar HadoopXmlExtractor.XmlExtractor /xmlextraction/seqfiles /xmlextraction/inventory ../testdata/ExtractInventory.xml
hadoop fs -cat /xmlextraction/inventory/part-r-00000

hadoop jar ../target/HadoopXmlExtractor-1.0-SNAPSHOT.jar HadoopXmlExtractor.XmlExtractor /xmlextraction/seqfiles /xmlextraction/book ../testdata/ExtractBook.xml
hadoop fs -cat /xmlextraction/book/part-r-00000
