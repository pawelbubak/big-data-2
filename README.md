# Big data - Spark - airbnb
## Instrukcja uruchomienia projektu
1. Należy skopiować pliki sh, jar i sql na klaster
2. Plikom sh należy nadać uprawnienia do wykonywania
    ```console
    foo@bar:~$ chmod +x *.sh
    ```
3. W celu załadowania zbioru danych do hadoop'a należy uruchomić skrypt `DownloadData.sh`. Skrypt ten pobierze 
 paczkę z zbiorem danych i rozpakuje ją w folderze `project-data`, a następnie dane zostaną załadowane do folderu
 `project/spark/` w systemie plików hdfs.
    ```console
     foo@bar:~$ ./DownloadData.sh
     ```
4. W celu utworzenia tabel przechowujących dane, należy uruchomić skrypt `CreateDatabase.sh`. Skrypt do uruchomienia
 wymaga pliku `CreateDatabase.sql`
    ```console
     foo@bar:~$ ./CreateDatabase.sh
     ```
5. W celu przetworzenia i załadowania danych do hurtowni należy uruchomić skrypt `FillTables.sh`. Skrypt wymaga 
 plików: `fact.jar`, `location.jar`, `location-score.jar`, `price.jar` i `time.jar`
    ```console
     foo@bar:~$ ./FillTables.sh
     ```
