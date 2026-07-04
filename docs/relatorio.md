# Relatório — 2º Trabalho de MC714 (Sistemas Distribuídos)

**Participantes:** Lucas Gabriel Monteiro — RA: _(preencher)_
**Repositório:** <https://github.com/code-lucasgabriel/raft-replicated-db>
**Vídeo:** _(preencher link)_

---

## 1. O problema

O problema escolhido é a **replicação consistente de um banco de dados chave-valor** entre múltiplas máquinas: manter N réplicas de um mesmo estado, aceitando escritas mesmo com falhas de nós, sem que as réplicas divirjam. Esse é o problema central de sistemas como etcd, Consul e ZooKeeper.

Sobre esse sistema, implementamos os três algoritmos pedidos no enunciado, todos com troca de mensagens real pela rede (gRPC sobre HTTP/2 e o transporte TCP do Raft):

| Algoritmo | Papel no sistema |
|---|---|
| **Consenso distribuído / eleição de líder** — Raft | replica cada escrita por um log comprometido por quórum; elege novo líder quando o atual falha |
| **Relógio lógico de Lamport** | carimba todas as mensagens do sistema (RPCs de clientes, entradas do log replicado, mensagens do mutex), estabelecendo ordem causal |
| **Exclusão mútua distribuída** — Ricart-Agrawala | seção crítica global usada pela operação `Incr` (read-modify-write atômico no cluster) |

Os três não são independentes: o Ricart-Agrawala **precisa** dos timestamps de Lamport (com desempate por id de nó) para formar a ordem total que decide a prioridade entre pedidos concorrentes, e o `Incr` protegido pela seção crítica escreve através do Raft.

## 2. Os algoritmos

### 2.1 Relógio lógico de Lamport (Lamport, 1978)

Cada processo — os três nós **e o cliente CLI** — mantém um contador com duas regras:

- **Evento local / envio:** `t = t + 1`, e a mensagem sai carimbada com `t` (`Tick`).
- **Recepção:** `t = max(t_local, t_mensagem) + 1` (`Observe`).

Isso garante a propriedade de causalidade: se o evento *a* aconteceu-antes de *b* (happened-before), então `ts(a) < ts(b)`. Três canais de mensagens carregam timestamps na nossa implementação:

1. **cliente ↔ nó** — todos os requests/responses gRPC têm um campo `lamport_time`;
2. **líder → réplicas** — cada comando proposto ao Raft carrega `(Time, Origin)`; quando o `FSM.Apply` executa a entrada comprometida em cada réplica, ele faz `Observe(Time)`. Ou seja: tratamos cada entrada do log replicado como uma mensagem do líder para cada réplica, que é exatamente o que ela é;
3. **nó ↔ nó (mutex)** — o REQUEST do Ricart-Agrawala carrega o timestamp do pedido e a resposta (grant) carrega o relógio de quem concedeu.

A ordem de Lamport é **parcial**: `ts(a) < ts(b)` não implica que *a* causou *b*. Para o mutex precisamos de ordem **total**, obtida comparando pares `(timestamp, id do nó)` — ids são únicos, então não há empate.

### 2.2 Exclusão mútua: Ricart-Agrawala (Ricart & Agrawala, 1981)

Para entrar na seção crítica (SC), um nó:

1. carimba **um** REQUEST com seu relógio de Lamport (o timestamp fica fixo durante todo o pedido);
2. envia o REQUEST aos N−1 pares e espera o *grant* de **todos**;
3. ao sair da SC, libera os grants que tinha adiado.

Quem recebe um REQUEST concede imediatamente, **exceto** se está dentro da SC ou se está pedindo com par `(timestamp, id)` menor — nesses casos **adia** a resposta até sair da SC. Como a ordem sobre os pares é total, exatamente um dos pedidos concorrentes coleta todos os grants primeiro (segurança), e o pedido globalmente menor nunca é adiado por todos (ausência de deadlock/starvation). São 2·(N−1) mensagens por entrada na SC — o ótimo provado no artigo.

**Mapeamento para RPC:** o par REQUEST/REPLY do artigo vira **uma única chamada gRPC bloqueante** (`Mutex.RequestCS`): a chamada carrega o REQUEST; o retorno é o grant; *adiar* é simplesmente não responder ainda. A fila de respostas adiadas vira uma lista de canais Go fechados no `Unlock`.

Dois detalhes de corretude que a implementação garante sob um único mutex local:

- o timestamp do pedido é fixado **antes** de qualquer REQUEST sair e não muda no meio do pedido — os dois lados de cada conflito comparam pares idênticos;
- o `Observe` do timestamp recebido acontece **antes** da decisão conceder/adiar — se o nó A viu o pedido de B antes de pedir, o timestamp de A será estritamente maior, e os dois lados concordam sobre quem veio primeiro.

### 2.3 Consenso / eleição de líder: Raft (Ongaro & Ousterhout, 2014)

O Raft replica um log de comandos: o líder recebe escritas, adiciona ao seu log, replica por `AppendEntries` e considera a entrada *comprometida* quando a maioria confirma; então cada nó aplica a entrada à sua máquina de estados (nosso KV em memória). Quando o líder falha, os seguidores detectam a ausência de heartbeat e disparam eleição (`RequestVote`); o candidato com log mais atualizado e maioria de votos vira líder — no nosso cluster de 3 nós, qualquer 2 formam quórum.

Para o consenso usamos a biblioteca **`hashicorp/raft`** (a mesma usada em produção pelo Consul e pelo Nomad), com persistência do log e do estado de votação em BoltDB — ver a Seção 4 sobre o que é biblioteca e o que é código nosso.

## 3. Arquitetura

Três nós idênticos em contêineres Docker, dois listeners por nó, sem coordenador externo:

```
                  ┌───────────────────────┐         ┌───────────────────────┐
   client ─gRPC─▶ │ node-1                │ ─raft─▶ │ node-2                │
                  │  :5000  DB+Mutex gRPC │         │  :5000  DB+Mutex gRPC │
                  │  :7000  raft TCP      │ ◀─raft─ │  :7000  raft TCP      │
                  │  /var/lib/raft-db     │         │  /var/lib/raft-db     │
                  └─────────┬─────────────┘         └───────────┬───────────┘
                            │ raft (AppendEntries, RequestVote, InstallSnapshot)
                            ▼
                  ┌───────────────────────┐
                  │ node-3                │
                  └───────────────────────┘
```

- **Porta 5000 (gRPC):** serviço `DB` para clientes (`Get`, `Put`, `Delete`, `Incr`) e serviço `Mutex` entre pares (Ricart-Agrawala + encaminhamento de escrita ao líder).
- **Porta 7000 (TCP):** transporte nativo do `hashicorp/raft` (`AppendEntries`, `RequestVote`, `InstallSnapshot`).

### 3.1 Fluxo de uma escrita (`Put`)

```
cliente ──Put(k,v)──▶ nó X
  X não é líder?  responde LeaderHint=<id do líder>; cliente repete lá
  X é líder:      cmd = {op:put, key, value, Time:Tick(), Origin:X}
                  raft.Apply(cmd) ── AppendEntries ──▶ maioria confirma
                  FSM.Apply em TODOS os nós: Observe(cmd.Time); store.Put(k,v)
                  líder responde OK (depois de aplicar localmente)
```

Leituras (`Get`) são locais à réplica consultada — rápidas, mas potencialmente defasadas (documentado; visto no experimento 5.3).

### 3.2 Fluxo do `Incr` (seção crítica)

`Incr` é um read-modify-write — a operação que uma API KV sem compare-and-swap **não** consegue fazer atomicamente — e é aceito por **qualquer** nó:

```
1. nó X recebe Incr(key)
2. ramutex.Lock()        REQUEST(ts,X) para todos; espera todos os grants
3. resolve o líder Raft
     X é líder:    raft.Barrier() → lê o valor → propõe Put(valor+1)
     X é seguidor: encaminha Incr ao líder via gRPC (sem re-travar!)
4. ramutex.Unlock()      libera os grants adiados
```

Dois pontos finos: o `Barrier` garante que um líder recém-eleito já aplicou todas as entradas comprometidas antes de ler (senão poderia ler valor velho *mesmo com o lock*); e o encaminhamento ao líder não tenta pegar o lock de novo — a SC já é do nó de origem; re-travar causaria deadlock, pois o líder esperaria um grant que o dono da SC não pode dar.

## 4. Implementação

- **Linguagem:** Go 1.26.
- **Comunicação:** gRPC (HTTP/2) com Protocol Buffers para cliente↔nó e nó↔nó (mutex + encaminhamento); transporte TCP próprio do `hashicorp/raft` para o consenso. Não há nenhuma simulação — todas as trocas são mensagens de rede reais.
- **Persistência:** BoltDB para o log do Raft e para `currentTerm`/`votedFor`; snapshots do FSM em arquivo.
- **Ambiente de execução:** Docker Compose com 3 contêineres (`node-1..3`), volumes nomeados por nó, portas de cliente mapeadas em `127.0.0.1:5001-5003`.

### 4.1 Fontes de código utilizadas (e o que foi alterado)

Conforme exigido pelo enunciado, as fontes externas e o que fizemos com cada uma:

| Fonte | Uso | Alterações |
|---|---|---|
| [`hashicorp/raft`](https://github.com/hashicorp/raft) v1.7.3 | Biblioteca de consenso: eleição de líder, replicação do log, commit por quórum, snapshots | **Nenhuma alteração no código da biblioteca.** Escrevemos a integração: a máquina de estados (`internal/fsm`), o armazenamento KV (`internal/store`), a montagem de stores/transporte/bootstrap (`internal/raftnode`) e o roteamento de escritas pelo líder (`internal/server`) |
| [`hashicorp/raft-boltdb`](https://github.com/hashicorp/raft-boltdb) v2.3.1 | `LogStore`/`StableStore` em BoltDB para o Raft | Nenhuma; usada como dependência |
| [`grpc-go`](https://github.com/grpc/grpc-go) / [`protobuf`](https://github.com/protocolbuffers/protobuf) | RPC e serialização | Nenhuma; usadas como dependência |
| Artigos de Lamport (1978) e Ricart & Agrawala (1981) | Especificação dos algoritmos | **Implementações escritas do zero** em `internal/lamport` e `internal/ramutex`, seguindo os artigos; a adaptação de REQUEST/REPLY para RPC bloqueante é nossa |

Ou seja: **o consenso vem de biblioteca (citada); o relógio de Lamport e o Ricart-Agrawala são implementação própria**, incluindo o transporte gRPC entre os nós.

### 4.2 Organização do código

```
proto/db/v1, proto/mutex/v1   esquemas protobuf (API de cliente e mutex entre pares)
internal/lamport              relógio de Lamport (Tick / Observe)          [próprio]
internal/ramutex              núcleo do Ricart-Agrawala, agnóstico a rede  [próprio]
internal/store                KV em memória (map + RWMutex)                [próprio]
internal/fsm                  raft.FSM: aplica comandos, snapshot/restore  [próprio]
internal/raftnode             montagem do hashicorp/raft                   [integração]
internal/server               serviços gRPC DB e Mutex; conexões a pares   [próprio]
internal/node                 raiz de composição                           [próprio]
cmd/main, cmd/client          binário do nó e cliente CLI                  [próprio]
```

O núcleo do `ramutex` fala com os pares só por uma interface `Transport` de um método; a implementação gRPC fica em `internal/server`. Isso permitiu testar o algoritmo real com um transporte em memória (ver 5.1).

## 5. Testes e experimentos

### 5.1 Testes de unidade (`make test`, com race detector)

- **`internal/lamport`:** monotonicidade, `Observe` = max+1, propriedade happened-before, 1000 `Tick`s concorrentes produzem timestamps únicos.
- **`internal/ramutex`:** os testes executam o algoritmo real sobre um transporte em processo:
  - *invariante de exclusão mútua:* 3 nós × 3 goroutines × 20 entradas na SC com detector de sobreposição por CAS atômico — nenhuma sobreposição em nenhuma execução;
  - *prevenção de lost update:* contador não sincronizado incrementado por read-modify-write sob o lock termina exato;
  - *timeout e rollback:* pedido com deadline enquanto outro nó segura a SC falha limpo e ambos os nós seguem utilizáveis;
  - *liberação de grants adiados* no `Unlock`.
- **`internal/fsm`:** apply de put/delete, rejeição de comandos malformados, snapshot/restore, merge do relógio no apply.

### 5.2 Experimento 1 — exclusão mútua e lost updates

30 increments (`Incr`) na mesma chave, 3 workers concorrentes, requisições espalhadas pelos 3 nós (medido no cluster Docker):

| Modo | Valor final (esperado 30) | Updates perdidos | Vazão |
|---|---|---|---|
| **Sem lock** (`bench-incr -unsafe`) | **14** | 16 (53%) | 72,6 incr/s |
| **Com Ricart-Agrawala** (`bench-incr`) | **30** | 0 | 78,1 incr/s |

Sem o lock, dois nós leem o mesmo valor e escrevem `v+1` um por cima do outro — mais da metade dos increments se perdeu. Com a SC, os read-modify-write serializam no cluster inteiro e o resultado é exato, com vazão equivalente nesta escala (as mensagens do mutex custam sub-milissegundo na rede do compose). Os logs dos nós mostram o protocolo decidindo prioridade — durante a execução concorrente foram registrados **57 adiamentos de grant**, por exemplo:

```
ramutex: node-3 requesting CS (ts=202), asking 2 peers
ramutex: node-3 DEFERS grant to node-1 — their (220,node-1) vs our (202,node-3), state=wanted
ramutex: node-3 ENTERED CS (ts=202)
ramutex: node-3 EXITED CS, released 2 deferred grant(s)
```

### 5.3 Experimento 2 — falha do líder (eleição)

1. Cluster estável com `node-1` líder; `docker compose stop node-1`.
2. Em ~1 s os logs mostram `node-2: Candidate` e `node-3: Leader` — nova eleição concluída.
3. `put` enviado a `node-2` responde com redirect e **é comprometido por `node-3`**: o sistema continua aceitando escritas com 1 de 3 nós morto (quórum 2/3).
4. `docker compose start node-1`: volta como seguidor e **lê a chave escrita enquanto estava morto** — o líder o ressincronizou via `AppendEntries`.

### 5.4 Experimento 3 — o contraste de disponibilidade

Com `node-1` ainda morto, um `Incr` **com lock** falha por timeout: o Ricart-Agrawala exige grant de **todos** os pares, então um único nó morto bloqueia a SC inteira — enquanto o Raft, baseado em quórum, seguiu comprometendo escritas normalmente. Esse contraste é a observação mais interessante do trabalho: consenso por maioria tolera minoria falha; exclusão mútua clássica por permissão não tolera falha nenhuma (mitigações: quóruns de Maekawa, locks com lease — fora do escopo).

Também observável nos experimentos: leituras em seguidores são **eventualmente consistentes** — um `get` imediatamente após o `put` pode ler o valor antigo na réplica, convergindo em seguida (documentado em `architecture.md` §5, junto com a construção ReadIndex que resolveria isso).

## 6. Como compilar e executar

```sh
make proto            # gera os bindings a partir dos .proto (requer buf)
make build            # bin/node e bin/client
make test             # testes com -race
docker compose up --build   # sobe node-1, node-2, node-3

# demonstrações
bin/client put contador 0
bin/client bench-incr -n 30 -c 3 -unsafe contador   # perde updates
bin/client put contador 0
bin/client bench-incr -n 30 -c 3 contador           # exato, sempre
docker compose stop node-1                          # mate o líder e repita um put
```

## 7. Comentários sobre a experiência

- A parte mais sutil não foi nenhum algoritmo isolado, e sim as **composições**: (i) perceber que o log do Raft é um canal de mensagens legítimo para o relógio de Lamport (o `FSM.Apply` é uma recepção); (ii) o deadlock que surge se o `Incr` encaminhado ao líder tentar pegar o lock de novo; (iii) a janela em que um líder recém-eleito ainda não aplicou entradas comprometidas — que perderia updates *mesmo com o lock* — fechada com `raft.Barrier`.
- Modelar o REQUEST/REPLY do Ricart-Agrawala como RPC bloqueante simplificou muito o código (a "resposta adiada" é só uma resposta que ainda não foi enviada), ao custo de uma goroutine e um stream HTTP/2 estacionados por grant adiado — irrelevante com N=3.
- Usar uma biblioteca de produção para o Raft e implementar os outros dois algoritmos à mão foi uma troca consciente: o valor de aprendizado do Raft aqui está na integração (FSM determinística, persistência do estado de votação, bootstrap) e nos modos de falha, que os experimentos exercitam diretamente.

## 8. Referências

1. L. Lamport. *Time, Clocks, and the Ordering of Events in a Distributed System.* CACM 21(7), 1978.
2. G. Ricart, A. K. Agrawala. *An Optimal Algorithm for Mutual Exclusion in Computer Networks.* CACM 24(1), 1981.
3. D. Ongaro, J. Ousterhout. *In Search of an Understandable Consensus Algorithm.* USENIX ATC, 2014. <https://raft.github.io/raft.pdf>
4. Biblioteca `hashicorp/raft`: <https://github.com/hashicorp/raft> (v1.7.3) e `hashicorp/raft-boltdb`: <https://github.com/hashicorp/raft-boltdb> (v2.3.1).
5. gRPC: <https://grpc.io>; Protocol Buffers: <https://protobuf.dev>.
6. Documentação de arquitetura do projeto: `architecture.md` neste repositório.
