# ML-01: Şablonlama doğruluğu (Loghub HDFS 2k)

`ml.evaluate_parsing` üretir. Ölçüt: Grouping Accuracy (Zhu vd., 2019).
Örnek: 2,000 satır, 14 gerçek şablon. Commit: e6672ae

| Benzerlik eşiği (sim_th) | Bulunan şablon | Gruplama doğruluğu | Seçilen |
|---|---|---|---|
| 0.3 | 14 | 1.0000 |  |
| 0.4 | 14 | 1.0000 |  |
| 0.5 | 14 | 1.0000 | ✔ |
| 0.6 | 14 | 1.0000 |  |
| 0.7 | 14 | 1.0000 |  |

## Seçilen ayarla (sim_th=0.5) bulunan şablonlar

- `PacketResponder <NUM> for block <BLK> terminating`
- `BLOCK* NameSystem.addStoredBlock: blockMap updated: <IP> is added to <BLK> size <NUM>`
- `Received block <BLK> of size <NUM> from <IP>`
- `Receiving block <BLK> src: <IP> dest: <IP>`
- `BLOCK* NameSystem.allocateBlock: <*> <BLK>`
- `Verification succeeded for <BLK>`
- `Deleting block <BLK> file <*>`
- `<IP> Served block <BLK> to <IP>`
- `<IP>:Got exception while serving <BLK> to <IP>:`
- `BLOCK* NameSystem.delete: <BLK> is added to invalidSet of <IP>`
- `<IP> Starting thread to transfer block <BLK> to <IP>`
- `BLOCK* ask <IP> to delete <BLK>`
- `Received block <BLK> src: <IP> dest: <IP> of size <NUM>`
- `BLOCK* ask <IP> to replicate <BLK> to datanode(s) <IP>`

## Tam HDFS_v1

11,175,629 satırda **46** şablon bulundu; Loghub'ın resmi listesinde **29** şablon var. Fark çoğunlukla seyrek istisna mesajlarından gelir: Drain istisna türüne göre ayrı şablon açar, resmi liste bunları tek `<*>` altında toplar.

| Satır sayısı | Şablon |
|---|---|
| 1,723,232 | `Receiving block <BLK> src: <IP> dest: <IP>` |
| 1,719,741 | `BLOCK* NameSystem.addStoredBlock: blockMap updated: <IP> is added to <BLK> size <NUM>` |
| 1,706,728 | `PacketResponder <NUM> for block <BLK> <*>` |
| 1,706,514 | `Received block <BLK> of size <NUM> from <IP>` |
| 1,402,047 | `Deleting block <BLK> file <*>` |
| 1,396,174 | `BLOCK* NameSystem.delete: <BLK> is added to invalidSet of <IP>` |
| 575,061 | `BLOCK* NameSystem.allocateBlock: <*> <BLK>` |
| 428,726 | `<IP> Served block <BLK> to <IP>` |
| 356,207 | `<IP>:Got exception while serving <BLK> to <IP>:` |
| 120,036 | `Verification succeeded for <BLK>` |
| 7,097 | `Received block <BLK> src: <IP> dest: <IP> of size <NUM>` |
| 6,937 | `<IP>:Transmitted block <BLK> to <IP>` |
| 6,837 | `BLOCK* ask <IP> to replicate <BLK> to datanode(s) <IP>` |
| 6,837 | `<IP> Starting thread to transfer block <BLK> to <IP>` |
| 5,545 | `Unexpected error trying to delete block <BLK>. BlockInfo not found in volumeMap.` |
| 3,226 | `writeBlock <BLK> received exception java.io.IOException: Could not read from stream` |
| 1,464 | `Receiving empty packet for block <BLK>` |
| 1,288 | `BLOCK* NameSystem.addStoredBlock: addStoredBlock request received for <BLK> on <IP> size <NUM> But it does not belong to any file.` |
| 975 | `BLOCK* NameSystem.addStoredBlock: Redundant addStoredBlock request received for <BLK> on <IP> size <NUM>` |
| 165 | `<IP> Starting thread to transfer block <BLK> to <IP>, <IP>` |
| 165 | `BLOCK* ask <IP> to replicate <BLK> to datanode(s) <IP> <IP>` |
| 75 | `Exception in receiveBlock for block <BLK> java.io.IOException: Connection reset by peer` |
| 75 | `writeBlock <BLK> received exception java.io.IOException: Connection reset by peer` |
| 67 | `writeBlock <BLK> received exception <*>` |
| 65 | `Changing block file offset of block <BLK> from <NUM> to <NUM> meta file offset to <NUM>` |
| 56 | `Exception in receiveBlock for block <BLK> <*>` |
| 47 | `PendingReplicationMonitor timed out block <BLK>` |
| 34 | `<IP>:Exception writing block <BLK> to mirror <IP>` |
| 33 | `PacketResponder <BLK> <NUM> Exception java.io.InterruptedIOException: Interruped while waiting for IO on channel java.nio.channels.SocketCha` |
| 29 | `PacketResponder <BLK> <NUM> Exception <*>` |
| 22 | `PacketResponder <BLK> <NUM> Exception java.net.SocketTimeoutException: <NUM> millis timeout while waiting for channel to be ready for read. ` |
| 16 | `Exception in receiveBlock for block <BLK> java.io.InterruptedIOException: Interruped while waiting for IO on channel java.nio.channels.Socke` |
| 16 | `writeBlock <BLK> received exception java.io.IOException: <*> <*>` |
| 15 | `writeBlock <BLK> received exception java.net.SocketTimeoutException: <NUM> millis timeout while waiting for channel to be ready for <*> ch :` |
| 13 | `PacketResponder <BLK> <NUM> Exception java.io.IOException: <*> <*> <*> <*>` |
| 10 | `Adding an already existing block <BLK>` |
| 9 | `writeBlock <BLK> received exception java.io.IOException: Block <BLK> is valid, and cannot be written to.` |
| 9 | `<IP>:Failed to transfer <BLK> to <IP> got java.io.IOException: Connection reset by peer` |
| 7 | `writeBlock <BLK> received exception java.io.InterruptedIOException: Interruped while waiting for IO on channel java.nio.channels.SocketChann` |
| 6 | `PacketResponder <BLK> <NUM> Exception java.io.IOException: Broken pipe` |
| 5 | `Exception in receiveBlock for block <BLK> java.net.SocketTimeoutException: <NUM> millis timeout while waiting for channel to be ready for wr` |
| 5 | `Reopen Block <BLK>` |
| 5 | `PacketResponder <BLK> <NUM> Exception java.io.InterruptedIOException: Interruped while waiting for IO on channel java.nio.channels.SocketCha` |
| 4 | `BLOCK* Removing block <BLK> from neededReplications as it does not belong to any file.` |
| 3 | `Exception in receiveBlock for block <BLK> java.io.IOException: Broken pipe` |
| 1 | `writeBlock <BLK> received exception java.net.NoRouteToHostException: No route to host` |
