# Kafka Streams

This project demonstrates a basic Kafka Streams application using Java. All stream processing logic is implemented in `BusFareStreamsApp.java`.

## 📁 Project Structure
``` 
  kafka-streams-demo/ 
  ├── pom.xml 
  └── src/ 
  └── main/ 
  └── java/ 
  └── BusFareStreamsApp.java
``` 


- `pom.xml`: Contains all Maven dependencies.
- `BusFareStreamsApp.java`: Main class for the Kafka Streams application.

---

## 🧰 Prerequisites

- **Java**: 21 (Tested with Java 21.0.5 LTS)

  
- **Maven**: Install via Homebrew

```bash
brew install maven
```

⚙️ Build Instructions

After every change to the code or dependencies, rebuild the project:

```bash
mvn clean install
```

▶️ Run the Application

Execute the Kafka Streams app:

```bash
mvn exec:java -Dexec.mainClass=BusFareStreamsApp
```



