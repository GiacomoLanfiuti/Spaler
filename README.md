## Spaler: Spark and GraphX based de novo genome assembler

### Introduzione alla tematica
Il lavoro svolto si basa sull'articolo di A. Abu-Doleh and Ü. V. Çatalyürek presentato alla IEEE International Conference on Big Data nel 2015. 
Il paper presenta una possibile soluzione ad un noto problema studiato in ambito genetico: l’assemblaggio del genoma de novo. Con ciò si intende la ricostruzione del genoma, ovvero la totalità delle informazioni genetiche contenute nel DNA di un individuo, partendo solamente da alcuni frammenti di quest’ultimo. Tale assemblaggio si basa sulla possibilità di sovrapporre tali segmenti al fine di ricostruire sequenze più lunghe di DNA.

### Elementi nel repository

* `SRC`: contiene le classi .java e scala necessarie per l'implementazione.

Classi| Descrizione
---- | ----
Spaler|Classe contenente il main
Arco|Classe per la creazione degli oggetti di tipo arco
ArcoPol|Classe che estende la classe **Arco** aggiungendo l'informazione sulla polarità
CreaArchi|Classe che implementa una PairFlatMapFunction contenente il metodo per generare gli archi del grafo
CreaRep|Classe che implementa una PairFlatMapFunction contenente il metodo per generare i vertici del grafo
ScalaPregel| Classe in Scala contenente i metodi per la gestione del GraphX

Inoltre, sono presenti le classi necessarie da avere in modo tale da poter importare i file di tipo .fastq nell'implementazione: Record, QRecord, PartialSequence, FASTQInputFileFormat, FASTQReadsRecordRecorder.

* `File Gradle`: necessario per l'utilizzo dell'IDE Intellij. Sono presenti le dependency utilizzate.
* `Data`: contiene i file .fatsq contenenti le sequenza genomiche da analizzare.


### Esposizione algoritmo
L'algoritmo, basato sul paradigma Map-Reduce, si sviluppa in due parti: la prima parte si occupa della creazione del grafo di De Brujin e la sua rappresentazione su Neo4J, mentre la seconda sviluppa la creazione delle *contig*, cioè lunghe sequenze di basi azotate, tramite un processo pregel based.



### Istruzioni d'installazione e uso
Per utilizzare il progetto Spaler si suggerisce l'utilizzo di Intellij che permette l'uso contemporaneo dei linguaggi Java e Scala. Per fare questo è necessario creare un progetto Java e installare il plugin di Scala.
Versione 2021.2.1 per Intellij: https://www.jetbrains.com/idea/download/#section=windows

É presente nel repository il file gradle con le dependency necessarie per l'implementazione corretta del algoritmo, quindi basterà scaricare ed inserire il file all'interno del proprio progetto.
L'implementazione dell'algortimo viene integrata con il database NoSQL Neo4J, tramite il quale viene ottenuta una rappresentazione grafica del Grafo di De Brujin.  
Versione Neo4j Enterprise 4.3.3 per Neo4j: https://neo4j.com/download/

Consigliamo per l'utilizzo del codice di creare un progetto su Intellij e inserire all'interno della cartella che si genera i file presenti nel repository.


### Teconologie utilizzate
Nella lavorazione è stato utilizzato l'ambiente di sviluppo integrato Intellij e i linguaggi di programmazione Java e Scala. 
Per l'implementazione distribuita si è riscorso all'utilizzo di Apache Spark e GraphX.
Infine, per la rappresentazioen grafica è stato utilizzato il database NoSQL Neo4J.


