call mvn clean package
call hadoop fs -mkdir /xmlextraction
call hadoop fs -mkdir /xmlextraction/stores
call hadoop fs -rm -skipTrash /xmlextraction/inventory/part-r-00000
call hadoop fs -rm -skipTrash /xmlextraction/inventory/_SUCCESS
call hadoop fs -rmdir /xmlextraction/inventory
call hadoop fs -rm -skipTrash /xmlextraction/book/part-r-00000
call hadoop fs -rm -skipTrash /xmlextraction/book/_SUCCESS
call hadoop fs -rmdir /xmlextraction/book


call hadoop fs -copyFromLocal ./testdata/InventorySoho.xml /xmlextraction/stores
call hadoop fs -copyFromLocal ./testdata/InventoryBanks.xml /xmlextraction/stores
call hadoop fs -ls /xmlextraction/stores
call hadoop fs -cat /xmlextraction/stores/InventorySoho.xml
call hadoop fs -cat /xmlextraction/stores/InventoryBanks.xml

call hadoop jar ./target/HadoopXmlExtractor-1.0-SNAPSHOT.jar HadoopXmlExtractor.CreateSequenceFile /xmlextraction/stores/ /xmlextraction/seqfiles/inventory.seq
call hadoop fs -ls /xmlextraction/seqfiles

call hadoop jar ./target/HadoopXmlExtractor-1.0-SNAPSHOT.jar HadoopXmlExtractor.XmlExtractor /xmlextraction/seqfiles /xmlextraction/inventory ./testdata/ExtractInventory.xml
call hadoop fs -cat /xmlextraction/inventory/part-r-00000

call hadoop jar ./target/HadoopXmlExtractor-1.0-SNAPSHOT.jar HadoopXmlExtractor.XmlExtractor /xmlextraction/seqfiles /xmlextraction/book ./testdata/ExtractBook.xml
call hadoop fs -cat /xmlextraction/book/part-r-00000