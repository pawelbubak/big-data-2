# Big data - Spark - airbnb
## Instrukcja uruchomienia projektu
1. Należy skopiować pliki sh, jar i sql na klaster
    ```console
    foo@bar:~$ gsutil cp gs://nazwa_kubelka/folder/*.sh ./
    foo@bar:~$ gsutil cp gs://nazwa_kubelka/folder/*.jar ./
    foo@bar:~$ gsutil cp gs://nazwa_kubelka/folder/*.sql ./
    ```
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

## Generacja plików jar
1. W katalogu sources znajdują się projekty poszczególnych procesów ETL, aby IntelliJ mogło je obsłużyć należy dodać je
 jako moduły `File->Project Structure->Modules (Import module->wybierz folder->sbt)` lub używając zakładki `sbt` 
2. Aby móc wygenerować pliki jar należy odpowiednio skonfigurować środowisko w zakładce
 `File->Project Structure->Artifacts`. W celu konfiguracji generacji wybranego pliku jar należy kliknąć `+` i wybrać z
  listy `JAR->Empty`, następnie nadać mu odpowienią nazwę oraz wybrać z zakładki `Available Elements` odpowiedni projekt
  np. `fact->'fact' compiled output`
3. W celu wygenerowania plików jar używając paska narzędzi wybieramy `Build->Build Artifacts..`, a następnie wybieramy
 odpowiedni projekt i klikamy jedną z opcji (np. `Build`)
4. Plik powininen zostać wygenerowany w folderze `out/artifacts/nazwa_projektu`
