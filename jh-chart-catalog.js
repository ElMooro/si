/* Chart data catalog — classify JustHodl harvests, search them, plot full history when the warehouse has it.
 * FRED/NY Fed → PROXY /series (warehouse history). CryptoQuant → data/cryptoquant-series.json (harvest points).
 * CISS → data/ciss-stress.json. Snapshots (13F, FMP TTM) open desks, never invented daily history.
 */
(function (global) {
  "use strict";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  var CQ = null, CISS = null, SYM = null, IND = null, INST = null, PROV = null, IDX_P = null;
  var WAREHOUSE_CACHES=new Map(),WAREHOUSE_CACHE_CAPACITY=8;
  var EXCH = { NASDAQ:1, NYSE:1, AMEX:1, ARCA:1, CBOE:1, TVC:1, BINANCE:1, INDEX:1, FX:1, CRYPTO:1, CME:1, COMEX:1, NYMEX:1, OTC:1, BATS:1, IEX:1, OPRA:1 };
  var DEFILLAMA_IDS = new Map(["defillama:tvl:0G","defillama:tvl:AB","defillama:tvl:ADI","defillama:tvl:AFX%20L1","defillama:tvl:AILayer","defillama:tvl:ALEO","defillama:tvl:ALV","defillama:tvl:AO","defillama:tvl:Abstract","defillama:tvl:Acala","defillama:tvl:Aeternity","defillama:tvl:Agoric","defillama:tvl:AirDAO","defillama:tvl:Aleph%20Zero%20EVM","defillama:tvl:Alephium","defillama:tvl:Algorand","defillama:tvl:Althea","defillama:tvl:Ancient8","defillama:tvl:Anubis","defillama:tvl:ApeChain","defillama:tvl:Aptos","defillama:tvl:Arbitrum","defillama:tvl:Arbitrum%20Nova","defillama:tvl:Arc","defillama:tvl:Archway","defillama:tvl:Areum%20Network","defillama:tvl:Artela","defillama:tvl:Asset%20Chain","defillama:tvl:Astar","defillama:tvl:Astar%20zkEVM","defillama:tvl:Aura%20Network","defillama:tvl:Aurora","defillama:tvl:Avalanche","defillama:tvl:Axiome","defillama:tvl:BCHyper","defillama:tvl:BESC%20Hyperchain","defillama:tvl:BEVM","defillama:tvl:BOB","defillama:tvl:BOT%20Chain","defillama:tvl:BSC","defillama:tvl:BSquared","defillama:tvl:Babylon%20Genesis","defillama:tvl:Bahamut","defillama:tvl:Base","defillama:tvl:Beam","defillama:tvl:Berachain","defillama:tvl:Bifrost","defillama:tvl:Bifrost%20Network","defillama:tvl:Binance","defillama:tvl:Bitcichain","defillama:tvl:Bitcoin","defillama:tvl:Bitcoincash","defillama:tvl:Bitgert","defillama:tvl:Bitindi","defillama:tvl:Bitlayer","defillama:tvl:Bitnet","defillama:tvl:Bitrock","defillama:tvl:Bittensor","defillama:tvl:Bittensor%20EVM","defillama:tvl:Bittorrent","defillama:tvl:Blast","defillama:tvl:Boba","defillama:tvl:Boba_Avax","defillama:tvl:Boba_Bnb","defillama:tvl:Bone","defillama:tvl:Bostrom","defillama:tvl:Botanix","defillama:tvl:BounceBit","defillama:tvl:CLV","defillama:tvl:CMP","defillama:tvl:CORE","defillama:tvl:CSC","defillama:tvl:Cabal","defillama:tvl:Callisto","defillama:tvl:Camp%20Network","defillama:tvl:Canto","defillama:tvl:Canton","defillama:tvl:Capx%20Chain","defillama:tvl:Carbon","defillama:tvl:Cardano","defillama:tvl:Celo","defillama:tvl:Chainflip","defillama:tvl:Chia","defillama:tvl:Chihuahua","defillama:tvl:Chiliz","defillama:tvl:Chromia","defillama:tvl:Citrea","defillama:tvl:Civitia","defillama:tvl:Comdex","defillama:tvl:Concordium","defillama:tvl:Conflux","defillama:tvl:CookieChain","defillama:tvl:Corn","defillama:tvl:CosmosHub","defillama:tvl:Coti","defillama:tvl:Crab","defillama:tvl:Crescent","defillama:tvl:Cronos","defillama:tvl:Cronos%20zkEVM","defillama:tvl:CrossFi","defillama:tvl:Cube","defillama:tvl:Curio","defillama:tvl:Cyber","defillama:tvl:DChain","defillama:tvl:DFK","defillama:tvl:DFS%20Network","defillama:tvl:DSC","defillama:tvl:Darwinia","defillama:tvl:Dash","defillama:tvl:Data%20Network","defillama:tvl:DeFiChain%20EVM","defillama:tvl:DeFiVerse","defillama:tvl:DefiChain","defillama:tvl:Degen","defillama:tvl:Dexalot","defillama:tvl:Dexit","defillama:tvl:Doge","defillama:tvl:Dogechain","defillama:tvl:Doma","defillama:tvl:DuckChain","defillama:tvl:Dungeon","defillama:tvl:Dymension","defillama:tvl:EDU%20Chain","defillama:tvl:ENI","defillama:tvl:ENULS","defillama:tvl:EOS%20EVM","defillama:tvl:ETHF","defillama:tvl:Echelon","defillama:tvl:Echelon%20Initia","defillama:tvl:Eclipse","defillama:tvl:Eden","defillama:tvl:Elastos","defillama:tvl:Electroneum","defillama:tvl:Elys","defillama:tvl:Elysium","defillama:tvl:Empire","defillama:tvl:Endurance","defillama:tvl:Energi","defillama:tvl:EnergyWeb","defillama:tvl:Equilibrium","defillama:tvl:Ergo","defillama:tvl:Eteria","defillama:tvl:Ethereal","defillama:tvl:Ethereum","defillama:tvl:EthereumClassic","defillama:tvl:EthereumPoW","defillama:tvl:Etherlink","defillama:tvl:Etica","defillama:tvl:Eventum","defillama:tvl:Everscale","defillama:tvl:Evmos","defillama:tvl:FSC","defillama:tvl:Fantom","defillama:tvl:Filecoin","defillama:tvl:Findora","defillama:tvl:Flare","defillama:tvl:Flow","defillama:tvl:Fluence","defillama:tvl:Fluent","defillama:tvl:Fogo","defillama:tvl:Form%20Network","defillama:tvl:Fraxtal","defillama:tvl:Fuel%20Ignition","defillama:tvl:FunctionX","defillama:tvl:Fuse","defillama:tvl:Fusion","defillama:tvl:GANchain","defillama:tvl:GGCHAIN","defillama:tvl:GOAT","defillama:tvl:GRX%20Chain","defillama:tvl:Gala","defillama:tvl:GateLayer","defillama:tvl:Genesys","defillama:tvl:Genshiro","defillama:tvl:Gnosis","defillama:tvl:GoChain","defillama:tvl:Godwoken","defillama:tvl:GodwokenV1","defillama:tvl:Goerli","defillama:tvl:Gravity%20by%20Galxe","defillama:tvl:HAQQ","defillama:tvl:HPB","defillama:tvl:Ham","defillama:tvl:Harmony","defillama:tvl:HashKey%20Chain","defillama:tvl:Haven1","defillama:tvl:HeLa","defillama:tvl:Heco","defillama:tvl:Hedera","defillama:tvl:Heiko","defillama:tvl:Hemi","defillama:tvl:Hoo","defillama:tvl:Horizen%20EON","defillama:tvl:Hydra","defillama:tvl:Hydra%20Chain","defillama:tvl:Hydration","defillama:tvl:Hyperliquid%20L1","defillama:tvl:ICP","defillama:tvl:INRI","defillama:tvl:IOTA","defillama:tvl:IOTA%20EVM","defillama:tvl:Icon","defillama:tvl:Igra","defillama:tvl:Immutable%20zkEVM","defillama:tvl:Inertia","defillama:tvl:Initia","defillama:tvl:Injective","defillama:tvl:Ink","defillama:tvl:Interlay","defillama:tvl:IoTeX","defillama:tvl:JBC","defillama:tvl:JOC","defillama:tvl:Joltify","defillama:tvl:Juno","defillama:tvl:K2","defillama:tvl:KCC","defillama:tvl:KUB","defillama:tvl:Kadena","defillama:tvl:Kaia","defillama:tvl:Kardia","defillama:tvl:Karura","defillama:tvl:Kasplex","defillama:tvl:Katana","defillama:tvl:Kava","defillama:tvl:Keeta","defillama:tvl:Kekchain","defillama:tvl:Kintsugi","defillama:tvl:Kopi","defillama:tvl:Kroma","defillama:tvl:Krown%20Network","defillama:tvl:Kujira","defillama:tvl:LUKSO","defillama:tvl:LaChain%20Network","defillama:tvl:Lachain","defillama:tvl:Lamden","defillama:tvl:Lens","defillama:tvl:Libre","defillama:tvl:LightLink","defillama:tvl:Linea","defillama:tvl:Lisk","defillama:tvl:Litecoin","defillama:tvl:Loop","defillama:tvl:Lung","defillama:tvl:MANTRA","defillama:tvl:MAP%20Protocol","defillama:tvl:MEER","defillama:tvl:MTT%20Network","defillama:tvl:MUUCHAIN","defillama:tvl:MVC","defillama:tvl:Manta","defillama:tvl:Manta%20Atlantic","defillama:tvl:Mantle","defillama:tvl:Massa","defillama:tvl:Matchain","defillama:tvl:Mayachain","defillama:tvl:MegaETH","defillama:tvl:Merlin","defillama:tvl:Meter","defillama:tvl:Metis","defillama:tvl:Mezo","defillama:tvl:Migaloo","defillama:tvl:Milkomeda%20A1","defillama:tvl:Milkomeda%20C1","defillama:tvl:Mind%20Network","defillama:tvl:Mint","defillama:tvl:Mixin","defillama:tvl:Mode","defillama:tvl:Monad","defillama:tvl:Moonbeam","defillama:tvl:Moonchain","defillama:tvl:Moonriver","defillama:tvl:Morph","defillama:tvl:Movement","defillama:tvl:MultiVAC","defillama:tvl:MultiversX","defillama:tvl:MyRx","defillama:tvl:NEO","defillama:tvl:NOS","defillama:tvl:Nahmii","defillama:tvl:Naka","defillama:tvl:Namada","defillama:tvl:Near","defillama:tvl:Neo%20X%20Mainnet","defillama:tvl:Neon","defillama:tvl:Neutron","defillama:tvl:Nibiru","defillama:tvl:Nolus","defillama:tvl:Nova%20Network","defillama:tvl:Nuls","defillama:tvl:OKTChain","defillama:tvl:OP%20Mainnet","defillama:tvl:OXFUN","defillama:tvl:Oasis%20Emerald","defillama:tvl:Oasis%20Sapphire","defillama:tvl:Oasys","defillama:tvl:Obyte","defillama:tvl:Odyssey","defillama:tvl:Omax","defillama:tvl:Ontology","defillama:tvl:OntologyEVM","defillama:tvl:Onus","defillama:tvl:Open","defillama:tvl:OpenGPU","defillama:tvl:Optimism","defillama:tvl:Oraichain","defillama:tvl:Osmosis","defillama:tvl:Palm","defillama:tvl:Parallel","defillama:tvl:Parex","defillama:tvl:Peaq","defillama:tvl:Pego","defillama:tvl:Penumbra","defillama:tvl:Pepu","defillama:tvl:Perennial","defillama:tvl:Persistence%20One","defillama:tvl:Pharos","defillama:tvl:Planq","defillama:tvl:Plasma","defillama:tvl:Plume","defillama:tvl:Plume%20Mainnet","defillama:tvl:Polis","defillama:tvl:Polkadex","defillama:tvl:Polygon","defillama:tvl:Polygon%20zkEVM","defillama:tvl:Polynomial","defillama:tvl:Prom","defillama:tvl:Provenance","defillama:tvl:PulseChain","defillama:tvl:Q%20Protocol","defillama:tvl:QIE","defillama:tvl:QL1","defillama:tvl:Quai","defillama:tvl:Qubic","defillama:tvl:REI","defillama:tvl:REIchain","defillama:tvl:RENEC","defillama:tvl:RISE","defillama:tvl:RSS3","defillama:tvl:Radix","defillama:tvl:Rangers","defillama:tvl:Rari","defillama:tvl:Rayls","defillama:tvl:Redbelly","defillama:tvl:Redstone","defillama:tvl:ReyaChain","defillama:tvl:Robinhood%20Chain","defillama:tvl:Rollux","defillama:tvl:Ronin","defillama:tvl:Rootstock","defillama:tvl:SKALE%20Europa","defillama:tvl:SX%20Network","defillama:tvl:SX%20Rollup","defillama:tvl:Saakuru","defillama:tvl:Saga","defillama:tvl:SatoshiVM","defillama:tvl:Scroll","defillama:tvl:Secret","defillama:tvl:Sei","defillama:tvl:Sentrix","defillama:tvl:Shape","defillama:tvl:Shibarium","defillama:tvl:Shiden","defillama:tvl:Shido","defillama:tvl:ShimmerEVM","defillama:tvl:Sifchain","defillama:tvl:Silicon%20zkEVM","defillama:tvl:Solana","defillama:tvl:Somnia","defillama:tvl:Soneium","defillama:tvl:Songbird","defillama:tvl:Sonic","defillama:tvl:Soon%20Network","defillama:tvl:Sophon","defillama:tvl:Sora","defillama:tvl:Stable","defillama:tvl:Stacks","defillama:tvl:Starcoin","defillama:tvl:Stargaze","defillama:tvl:Starknet","defillama:tvl:Stellar","defillama:tvl:Step","defillama:tvl:Strat","defillama:tvl:Stratis","defillama:tvl:Strato%20Chain","defillama:tvl:Sui","defillama:tvl:Superposition","defillama:tvl:Superseed","defillama:tvl:Supra","defillama:tvl:Swan","defillama:tvl:Swellchain","defillama:tvl:Syscoin","defillama:tvl:TAC","defillama:tvl:THORChain","defillama:tvl:TON","defillama:tvl:Taiko","defillama:tvl:Taraxa","defillama:tvl:Telos","defillama:tvl:Tempo","defillama:tvl:Tenet","defillama:tvl:Terra%20Classic","defillama:tvl:Terra2","defillama:tvl:Tezos","defillama:tvl:Theta","defillama:tvl:ThunderCore","defillama:tvl:Titan","defillama:tvl:Tlchain","defillama:tvl:Tombchain","defillama:tvl:Tron","defillama:tvl:UX","defillama:tvl:Ubiq","defillama:tvl:Ultron","defillama:tvl:Unichain","defillama:tvl:Unit%20Zero","defillama:tvl:Vana","defillama:tvl:Vara","defillama:tvl:Vaulta","defillama:tvl:VeChain","defillama:tvl:Velas","defillama:tvl:Venom","defillama:tvl:Verus","defillama:tvl:Viction","defillama:tvl:VinuChain","defillama:tvl:VirBiCoin","defillama:tvl:Vision","defillama:tvl:Vite","defillama:tvl:Voi%20Network","defillama:tvl:W%20Chain","defillama:tvl:WEMIX3.0","defillama:tvl:WINR%20Chain","defillama:tvl:Wanchain","defillama:tvl:Waterfall","defillama:tvl:Waves","defillama:tvl:Wax","defillama:tvl:World%20Chain","defillama:tvl:X%20Layer","defillama:tvl:XCHAIN","defillama:tvl:XDC","defillama:tvl:XION","defillama:tvl:XO","defillama:tvl:XPLA","defillama:tvl:XPR%20Network","defillama:tvl:XRPL","defillama:tvl:XRPL%20EVM","defillama:tvl:Xai","defillama:tvl:Xone%20Chain","defillama:tvl:Xphere","defillama:tvl:Yominet","defillama:tvl:ZIGChain","defillama:tvl:ZKsync%20Era","defillama:tvl:ZKsync%20Lite","defillama:tvl:ZYX","defillama:tvl:Zcash","defillama:tvl:Zeniq","defillama:tvl:Zero%20Network","defillama:tvl:ZetaChain","defillama:tvl:Zilliqa","defillama:tvl:Zircuit","defillama:tvl:Zkfair","defillama:tvl:Zora","defillama:tvl:aelf","defillama:tvl:all","defillama:tvl:dYdX","defillama:tvl:inEVM","defillama:tvl:opBNB","defillama:tvl:re.al","defillama:tvl:smartBCH","defillama:tvl:soonBase","defillama:tvl:svmBNB","defillama:tvl:zkLink%20Nova"].map(function(id){return [id.toLowerCase(),id];}));
  var REGIONAL_FED_IDS = new Map(["regionalfed:chicago-cfnai:CFNAI","regionalfed:chicago-cfnai:CFNAI_MA3","regionalfed:chicago-cfnai:C_H","regionalfed:chicago-cfnai:DIFFUSION","regionalfed:chicago-cfnai:EU_H","regionalfed:chicago-cfnai:P_I","regionalfed:chicago-cfnai:SO_I","regionalfed:dallas-manufacturing-nsa:Avgwk","regionalfed:dallas-manufacturing-nsa:Bact","regionalfed:dallas-manufacturing-nsa:Capu","regionalfed:dallas-manufacturing-nsa:Cexp","regionalfed:dallas-manufacturing-nsa:Colk","regionalfed:dallas-manufacturing-nsa:Dtm","regionalfed:dallas-manufacturing-nsa:Favgwk","regionalfed:dallas-manufacturing-nsa:Fbact","regionalfed:dallas-manufacturing-nsa:Fcapu","regionalfed:dallas-manufacturing-nsa:Fcexp","regionalfed:dallas-manufacturing-nsa:Fcolk","regionalfed:dallas-manufacturing-nsa:Fdtm","regionalfed:dallas-manufacturing-nsa:Ffgi","regionalfed:dallas-manufacturing-nsa:Fgi","regionalfed:dallas-manufacturing-nsa:Fgro","regionalfed:dallas-manufacturing-nsa:Fnemp","regionalfed:dallas-manufacturing-nsa:Fpfg","regionalfed:dallas-manufacturing-nsa:Fprm","regionalfed:dallas-manufacturing-nsa:Fprod","regionalfed:dallas-manufacturing-nsa:Fufil","regionalfed:dallas-manufacturing-nsa:Fvnwo","regionalfed:dallas-manufacturing-nsa:Fvshp","regionalfed:dallas-manufacturing-nsa:Fwgs","regionalfed:dallas-manufacturing-nsa:Gro","regionalfed:dallas-manufacturing-nsa:Nemp","regionalfed:dallas-manufacturing-nsa:Pfg","regionalfed:dallas-manufacturing-nsa:Prm","regionalfed:dallas-manufacturing-nsa:Prod","regionalfed:dallas-manufacturing-nsa:Ufil","regionalfed:dallas-manufacturing-nsa:Uncr","regionalfed:dallas-manufacturing-nsa:Vnwo","regionalfed:dallas-manufacturing-nsa:Vshp","regionalfed:dallas-manufacturing-nsa:Wgs","regionalfed:dallas-manufacturing-sa:Avgwk","regionalfed:dallas-manufacturing-sa:Bact","regionalfed:dallas-manufacturing-sa:Capu","regionalfed:dallas-manufacturing-sa:Cexp","regionalfed:dallas-manufacturing-sa:Colk","regionalfed:dallas-manufacturing-sa:Dtm","regionalfed:dallas-manufacturing-sa:Favgwk","regionalfed:dallas-manufacturing-sa:Fbact","regionalfed:dallas-manufacturing-sa:Fcapu","regionalfed:dallas-manufacturing-sa:Fcexp","regionalfed:dallas-manufacturing-sa:Fcolk","regionalfed:dallas-manufacturing-sa:Fdtm","regionalfed:dallas-manufacturing-sa:Ffgi","regionalfed:dallas-manufacturing-sa:Fgi","regionalfed:dallas-manufacturing-sa:Fgro","regionalfed:dallas-manufacturing-sa:Fnemp","regionalfed:dallas-manufacturing-sa:Fpfg","regionalfed:dallas-manufacturing-sa:Fprm","regionalfed:dallas-manufacturing-sa:Fprod","regionalfed:dallas-manufacturing-sa:Fufil","regionalfed:dallas-manufacturing-sa:Fvnwo","regionalfed:dallas-manufacturing-sa:Fvshp","regionalfed:dallas-manufacturing-sa:Fwgs","regionalfed:dallas-manufacturing-sa:Gro","regionalfed:dallas-manufacturing-sa:Nemp","regionalfed:dallas-manufacturing-sa:Pfg","regionalfed:dallas-manufacturing-sa:Prm","regionalfed:dallas-manufacturing-sa:Prod","regionalfed:dallas-manufacturing-sa:Ufil","regionalfed:dallas-manufacturing-sa:Uncr","regionalfed:dallas-manufacturing-sa:Vnwo","regionalfed:dallas-manufacturing-sa:Vshp","regionalfed:dallas-manufacturing-sa:Wgs","regionalfed:dallas-service-nsa:avgwk","regionalfed:dallas-service-nsa:bact","regionalfed:dallas-service-nsa:cexp","regionalfed:dallas-service-nsa:colk","regionalfed:dallas-service-nsa:emp","regionalfed:dallas-service-nsa:favgwk","regionalfed:dallas-service-nsa:fbact","regionalfed:dallas-service-nsa:fcexp","regionalfed:dallas-service-nsa:fcolk","regionalfed:dallas-service-nsa:femp","regionalfed:dallas-service-nsa:finp","regionalfed:dallas-service-nsa:fpemp","regionalfed:dallas-service-nsa:frev","regionalfed:dallas-service-nsa:fsell","regionalfed:dallas-service-nsa:fwgs","regionalfed:dallas-service-nsa:inp","regionalfed:dallas-service-nsa:pemp","regionalfed:dallas-service-nsa:rev","regionalfed:dallas-service-nsa:sell","regionalfed:dallas-service-nsa:uncr","regionalfed:dallas-service-nsa:wgs","regionalfed:dallas-service-sa:avgwk","regionalfed:dallas-service-sa:bact","regionalfed:dallas-service-sa:cexp","regionalfed:dallas-service-sa:colk","regionalfed:dallas-service-sa:emp","regionalfed:dallas-service-sa:favgwk","regionalfed:dallas-service-sa:fbact","regionalfed:dallas-service-sa:fcexp","regionalfed:dallas-service-sa:fcolk","regionalfed:dallas-service-sa:femp","regionalfed:dallas-service-sa:finp","regionalfed:dallas-service-sa:fpemp","regionalfed:dallas-service-sa:frev","regionalfed:dallas-service-sa:fsell","regionalfed:dallas-service-sa:fwgs","regionalfed:dallas-service-sa:inp","regionalfed:dallas-service-sa:pemp","regionalfed:dallas-service-sa:rev","regionalfed:dallas-service-sa:sell","regionalfed:dallas-service-sa:uncr","regionalfed:dallas-service-sa:wgs","regionalfed:kc-manufacturing:month-nsa:backlog","regionalfed:kc-manufacturing:month-nsa:capital-expenditures","regionalfed:kc-manufacturing:month-nsa:composite","regionalfed:kc-manufacturing:month-nsa:delivery-time","regionalfed:kc-manufacturing:month-nsa:employment","regionalfed:kc-manufacturing:month-nsa:export-orders","regionalfed:kc-manufacturing:month-nsa:finished-inventories","regionalfed:kc-manufacturing:month-nsa:materials-inventories","regionalfed:kc-manufacturing:month-nsa:new-orders","regionalfed:kc-manufacturing:month-nsa:prices-paid","regionalfed:kc-manufacturing:month-nsa:prices-received","regionalfed:kc-manufacturing:month-nsa:production","regionalfed:kc-manufacturing:month-nsa:shipments","regionalfed:kc-manufacturing:month-nsa:workweek","regionalfed:kc-manufacturing:month-sa:backlog","regionalfed:kc-manufacturing:month-sa:capital-expenditures","regionalfed:kc-manufacturing:month-sa:composite","regionalfed:kc-manufacturing:month-sa:delivery-time","regionalfed:kc-manufacturing:month-sa:employment","regionalfed:kc-manufacturing:month-sa:export-orders","regionalfed:kc-manufacturing:month-sa:finished-inventories","regionalfed:kc-manufacturing:month-sa:materials-inventories","regionalfed:kc-manufacturing:month-sa:new-orders","regionalfed:kc-manufacturing:month-sa:prices-paid","regionalfed:kc-manufacturing:month-sa:prices-received","regionalfed:kc-manufacturing:month-sa:production","regionalfed:kc-manufacturing:month-sa:shipments","regionalfed:kc-manufacturing:month-sa:workweek","regionalfed:kc-manufacturing:six-month-expectation-nsa:backlog","regionalfed:kc-manufacturing:six-month-expectation-nsa:capital-expenditures","regionalfed:kc-manufacturing:six-month-expectation-nsa:composite","regionalfed:kc-manufacturing:six-month-expectation-nsa:delivery-time","regionalfed:kc-manufacturing:six-month-expectation-nsa:employment","regionalfed:kc-manufacturing:six-month-expectation-nsa:export-orders","regionalfed:kc-manufacturing:six-month-expectation-nsa:finished-inventories","regionalfed:kc-manufacturing:six-month-expectation-nsa:materials-inventories","regionalfed:kc-manufacturing:six-month-expectation-nsa:new-orders","regionalfed:kc-manufacturing:six-month-expectation-nsa:prices-paid","regionalfed:kc-manufacturing:six-month-expectation-nsa:prices-received","regionalfed:kc-manufacturing:six-month-expectation-nsa:production","regionalfed:kc-manufacturing:six-month-expectation-nsa:shipments","regionalfed:kc-manufacturing:six-month-expectation-nsa:workweek","regionalfed:kc-manufacturing:six-month-expectation-sa:backlog","regionalfed:kc-manufacturing:six-month-expectation-sa:capital-expenditures","regionalfed:kc-manufacturing:six-month-expectation-sa:composite","regionalfed:kc-manufacturing:six-month-expectation-sa:delivery-time","regionalfed:kc-manufacturing:six-month-expectation-sa:employment","regionalfed:kc-manufacturing:six-month-expectation-sa:export-orders","regionalfed:kc-manufacturing:six-month-expectation-sa:finished-inventories","regionalfed:kc-manufacturing:six-month-expectation-sa:materials-inventories","regionalfed:kc-manufacturing:six-month-expectation-sa:new-orders","regionalfed:kc-manufacturing:six-month-expectation-sa:prices-paid","regionalfed:kc-manufacturing:six-month-expectation-sa:prices-received","regionalfed:kc-manufacturing:six-month-expectation-sa:production","regionalfed:kc-manufacturing:six-month-expectation-sa:shipments","regionalfed:kc-manufacturing:six-month-expectation-sa:workweek","regionalfed:kc-manufacturing:year-nsa:backlog","regionalfed:kc-manufacturing:year-nsa:capital-expenditures","regionalfed:kc-manufacturing:year-nsa:composite","regionalfed:kc-manufacturing:year-nsa:delivery-time","regionalfed:kc-manufacturing:year-nsa:employment","regionalfed:kc-manufacturing:year-nsa:export-orders","regionalfed:kc-manufacturing:year-nsa:finished-inventories","regionalfed:kc-manufacturing:year-nsa:materials-inventories","regionalfed:kc-manufacturing:year-nsa:new-orders","regionalfed:kc-manufacturing:year-nsa:prices-paid","regionalfed:kc-manufacturing:year-nsa:prices-received","regionalfed:kc-manufacturing:year-nsa:production","regionalfed:kc-manufacturing:year-nsa:shipments","regionalfed:kc-manufacturing:year-nsa:workweek","regionalfed:ny-empire-nsa:ASCDINA","regionalfed:ny-empire-nsa:ASCDNA","regionalfed:ny-empire-nsa:ASCINA","regionalfed:ny-empire-nsa:ASCNNA","regionalfed:ny-empire-nsa:ASFDINA","regionalfed:ny-empire-nsa:ASFDNA","regionalfed:ny-empire-nsa:ASFINA","regionalfed:ny-empire-nsa:ASFNNA","regionalfed:ny-empire-nsa:AWCDINA","regionalfed:ny-empire-nsa:AWCDNA","regionalfed:ny-empire-nsa:AWCINA","regionalfed:ny-empire-nsa:AWCNNA","regionalfed:ny-empire-nsa:AWFDINA","regionalfed:ny-empire-nsa:AWFDNA","regionalfed:ny-empire-nsa:AWFINA","regionalfed:ny-empire-nsa:AWFNNA","regionalfed:ny-empire-nsa:CEFDINA","regionalfed:ny-empire-nsa:CEFDNA","regionalfed:ny-empire-nsa:CEFINA","regionalfed:ny-empire-nsa:CEFNNA","regionalfed:ny-empire-nsa:DTCDINA","regionalfed:ny-empire-nsa:DTCDNA","regionalfed:ny-empire-nsa:DTCINA","regionalfed:ny-empire-nsa:DTCNNA","regionalfed:ny-empire-nsa:DTFDINA","regionalfed:ny-empire-nsa:DTFDNA","regionalfed:ny-empire-nsa:DTFINA","regionalfed:ny-empire-nsa:DTFNNA","regionalfed:ny-empire-nsa:GACDINA","regionalfed:ny-empire-nsa:GACDNA","regionalfed:ny-empire-nsa:GACINA","regionalfed:ny-empire-nsa:GACNNA","regionalfed:ny-empire-nsa:GAFDINA","regionalfed:ny-empire-nsa:GAFDNA","regionalfed:ny-empire-nsa:GAFINA","regionalfed:ny-empire-nsa:GAFNNA","regionalfed:ny-empire-nsa:IVCDINA","regionalfed:ny-empire-nsa:IVCDNA","regionalfed:ny-empire-nsa:IVCINA","regionalfed:ny-empire-nsa:IVCNNA","regionalfed:ny-empire-nsa:IVFDINA","regionalfed:ny-empire-nsa:IVFDNA","regionalfed:ny-empire-nsa:IVFINA","regionalfed:ny-empire-nsa:IVFNNA","regionalfed:ny-empire-nsa:NECDINA","regionalfed:ny-empire-nsa:NECDNA","regionalfed:ny-empire-nsa:NECINA","regionalfed:ny-empire-nsa:NECNNA","regionalfed:ny-empire-nsa:NEFDINA","regionalfed:ny-empire-nsa:NEFDNA","regionalfed:ny-empire-nsa:NEFINA","regionalfed:ny-empire-nsa:NEFNNA","regionalfed:ny-empire-nsa:NOCDINA","regionalfed:ny-empire-nsa:NOCDNA","regionalfed:ny-empire-nsa:NOCINA","regionalfed:ny-empire-nsa:NOCNNA","regionalfed:ny-empire-nsa:NOFDINA","regionalfed:ny-empire-nsa:NOFDNA","regionalfed:ny-empire-nsa:NOFINA","regionalfed:ny-empire-nsa:NOFNNA","regionalfed:ny-empire-nsa:PPCDINA","regionalfed:ny-empire-nsa:PPCDNA","regionalfed:ny-empire-nsa:PPCINA","regionalfed:ny-empire-nsa:PPCNNA","regionalfed:ny-empire-nsa:PPFDINA","regionalfed:ny-empire-nsa:PPFDNA","regionalfed:ny-empire-nsa:PPFINA","regionalfed:ny-empire-nsa:PPFNNA","regionalfed:ny-empire-nsa:PRCDINA","regionalfed:ny-empire-nsa:PRCDNA","regionalfed:ny-empire-nsa:PRCINA","regionalfed:ny-empire-nsa:PRCNNA","regionalfed:ny-empire-nsa:PRFDINA","regionalfed:ny-empire-nsa:PRFDNA","regionalfed:ny-empire-nsa:PRFINA","regionalfed:ny-empire-nsa:PRFNNA","regionalfed:ny-empire-nsa:SHCDINA","regionalfed:ny-empire-nsa:SHCDNA","regionalfed:ny-empire-nsa:SHCINA","regionalfed:ny-empire-nsa:SHCNNA","regionalfed:ny-empire-nsa:SHFDINA","regionalfed:ny-empire-nsa:SHFDNA","regionalfed:ny-empire-nsa:SHFINA","regionalfed:ny-empire-nsa:SHFNNA","regionalfed:ny-empire-nsa:UOCDINA","regionalfed:ny-empire-nsa:UOCDNA","regionalfed:ny-empire-nsa:UOCINA","regionalfed:ny-empire-nsa:UOCNNA","regionalfed:ny-empire-nsa:UOFDINA","regionalfed:ny-empire-nsa:UOFDNA","regionalfed:ny-empire-nsa:UOFINA","regionalfed:ny-empire-nsa:UOFNNA","regionalfed:ny-empire-sa:ASCDISA","regionalfed:ny-empire-sa:ASCDSA","regionalfed:ny-empire-sa:ASCISA","regionalfed:ny-empire-sa:ASCNSA","regionalfed:ny-empire-sa:ASFDISA","regionalfed:ny-empire-sa:ASFDSA","regionalfed:ny-empire-sa:ASFISA","regionalfed:ny-empire-sa:ASFNSA","regionalfed:ny-empire-sa:AWCDISA","regionalfed:ny-empire-sa:AWCDSA","regionalfed:ny-empire-sa:AWCISA","regionalfed:ny-empire-sa:AWCNSA","regionalfed:ny-empire-sa:AWFDISA","regionalfed:ny-empire-sa:AWFDSA","regionalfed:ny-empire-sa:AWFISA","regionalfed:ny-empire-sa:AWFNSA","regionalfed:ny-empire-sa:CEFDISA","regionalfed:ny-empire-sa:CEFDSA","regionalfed:ny-empire-sa:CEFISA","regionalfed:ny-empire-sa:CEFNSA","regionalfed:ny-empire-sa:DTCDISA","regionalfed:ny-empire-sa:DTCDSA","regionalfed:ny-empire-sa:DTCISA","regionalfed:ny-empire-sa:DTCNSA","regionalfed:ny-empire-sa:DTFDISA","regionalfed:ny-empire-sa:DTFDSA","regionalfed:ny-empire-sa:DTFISA","regionalfed:ny-empire-sa:DTFNSA","regionalfed:ny-empire-sa:GACDISA","regionalfed:ny-empire-sa:GACDSA","regionalfed:ny-empire-sa:GACISA","regionalfed:ny-empire-sa:GACNSA","regionalfed:ny-empire-sa:GAFDISA","regionalfed:ny-empire-sa:GAFDSA","regionalfed:ny-empire-sa:GAFISA","regionalfed:ny-empire-sa:GAFNSA","regionalfed:ny-empire-sa:IVCDISA","regionalfed:ny-empire-sa:IVCDSA","regionalfed:ny-empire-sa:IVCISA","regionalfed:ny-empire-sa:IVCNSA","regionalfed:ny-empire-sa:IVFDISA","regionalfed:ny-empire-sa:IVFDSA","regionalfed:ny-empire-sa:IVFISA","regionalfed:ny-empire-sa:IVFNSA","regionalfed:ny-empire-sa:NECDISA","regionalfed:ny-empire-sa:NECDSA","regionalfed:ny-empire-sa:NECISA","regionalfed:ny-empire-sa:NECNSA","regionalfed:ny-empire-sa:NEFDISA","regionalfed:ny-empire-sa:NEFDSA","regionalfed:ny-empire-sa:NEFISA","regionalfed:ny-empire-sa:NEFNSA","regionalfed:ny-empire-sa:NOCDISA","regionalfed:ny-empire-sa:NOCDSA","regionalfed:ny-empire-sa:NOCISA","regionalfed:ny-empire-sa:NOCNSA","regionalfed:ny-empire-sa:NOFDISA","regionalfed:ny-empire-sa:NOFDSA","regionalfed:ny-empire-sa:NOFISA","regionalfed:ny-empire-sa:NOFNSA","regionalfed:ny-empire-sa:PPCDISA","regionalfed:ny-empire-sa:PPCDSA","regionalfed:ny-empire-sa:PPCISA","regionalfed:ny-empire-sa:PPCNSA","regionalfed:ny-empire-sa:PPFDISA","regionalfed:ny-empire-sa:PPFDSA","regionalfed:ny-empire-sa:PPFISA","regionalfed:ny-empire-sa:PPFNSA","regionalfed:ny-empire-sa:PRCDISA","regionalfed:ny-empire-sa:PRCDSA","regionalfed:ny-empire-sa:PRCISA","regionalfed:ny-empire-sa:PRCNSA","regionalfed:ny-empire-sa:PRFDISA","regionalfed:ny-empire-sa:PRFDSA","regionalfed:ny-empire-sa:PRFISA","regionalfed:ny-empire-sa:PRFNSA","regionalfed:ny-empire-sa:SHCDISA","regionalfed:ny-empire-sa:SHCDSA","regionalfed:ny-empire-sa:SHCISA","regionalfed:ny-empire-sa:SHCNSA","regionalfed:ny-empire-sa:SHFDISA","regionalfed:ny-empire-sa:SHFDSA","regionalfed:ny-empire-sa:SHFISA","regionalfed:ny-empire-sa:SHFNSA","regionalfed:ny-empire-sa:UOCDISA","regionalfed:ny-empire-sa:UOCDSA","regionalfed:ny-empire-sa:UOCISA","regionalfed:ny-empire-sa:UOCNSA","regionalfed:ny-empire-sa:UOFDISA","regionalfed:ny-empire-sa:UOFDSA","regionalfed:ny-empire-sa:UOFISA","regionalfed:ny-empire-sa:UOFNSA","regionalfed:philadelphia-mbos:awcdfna","regionalfed:philadelphia-mbos:awcdfsa","regionalfed:philadelphia-mbos:awcdna","regionalfed:philadelphia-mbos:awcdsa","regionalfed:philadelphia-mbos:awcina","regionalfed:philadelphia-mbos:awcisa","regionalfed:philadelphia-mbos:awcnna","regionalfed:philadelphia-mbos:awcnsa","regionalfed:philadelphia-mbos:awfdfna","regionalfed:philadelphia-mbos:awfdfsa","regionalfed:philadelphia-mbos:awfdna","regionalfed:philadelphia-mbos:awfdsa","regionalfed:philadelphia-mbos:awfina","regionalfed:philadelphia-mbos:awfisa","regionalfed:philadelphia-mbos:awfnna","regionalfed:philadelphia-mbos:awfnsa","regionalfed:philadelphia-mbos:cefdfna","regionalfed:philadelphia-mbos:cefdfsa","regionalfed:philadelphia-mbos:cefdna","regionalfed:philadelphia-mbos:cefdsa","regionalfed:philadelphia-mbos:cefina","regionalfed:philadelphia-mbos:cefisa","regionalfed:philadelphia-mbos:cefnna","regionalfed:philadelphia-mbos:cefnsa","regionalfed:philadelphia-mbos:dtcdfna","regionalfed:philadelphia-mbos:dtcdfsa","regionalfed:philadelphia-mbos:dtcdna","regionalfed:philadelphia-mbos:dtcdsa","regionalfed:philadelphia-mbos:dtcina","regionalfed:philadelphia-mbos:dtcisa","regionalfed:philadelphia-mbos:dtcnna","regionalfed:philadelphia-mbos:dtcnsa","regionalfed:philadelphia-mbos:dtfdfna","regionalfed:philadelphia-mbos:dtfdfsa","regionalfed:philadelphia-mbos:dtfdna","regionalfed:philadelphia-mbos:dtfdsa","regionalfed:philadelphia-mbos:dtfina","regionalfed:philadelphia-mbos:dtfisa","regionalfed:philadelphia-mbos:dtfnna","regionalfed:philadelphia-mbos:dtfnsa","regionalfed:philadelphia-mbos:gacdfna","regionalfed:philadelphia-mbos:gacdfsa","regionalfed:philadelphia-mbos:gacdna","regionalfed:philadelphia-mbos:gacdsa","regionalfed:philadelphia-mbos:gacina","regionalfed:philadelphia-mbos:gacisa","regionalfed:philadelphia-mbos:gacnna","regionalfed:philadelphia-mbos:gacnsa","regionalfed:philadelphia-mbos:gafdfna","regionalfed:philadelphia-mbos:gafdfsa","regionalfed:philadelphia-mbos:gafdna","regionalfed:philadelphia-mbos:gafdsa","regionalfed:philadelphia-mbos:gafina","regionalfed:philadelphia-mbos:gafisa","regionalfed:philadelphia-mbos:gafnna","regionalfed:philadelphia-mbos:gafnsa","regionalfed:philadelphia-mbos:ivcdfna","regionalfed:philadelphia-mbos:ivcdfsa","regionalfed:philadelphia-mbos:ivcdna","regionalfed:philadelphia-mbos:ivcdsa","regionalfed:philadelphia-mbos:ivcina","regionalfed:philadelphia-mbos:ivcisa","regionalfed:philadelphia-mbos:ivcnna","regionalfed:philadelphia-mbos:ivcnsa","regionalfed:philadelphia-mbos:ivfdfna","regionalfed:philadelphia-mbos:ivfdfsa","regionalfed:philadelphia-mbos:ivfdna","regionalfed:philadelphia-mbos:ivfdsa","regionalfed:philadelphia-mbos:ivfina","regionalfed:philadelphia-mbos:ivfisa","regionalfed:philadelphia-mbos:ivfnna","regionalfed:philadelphia-mbos:ivfnsa","regionalfed:philadelphia-mbos:necdfna","regionalfed:philadelphia-mbos:necdfsa","regionalfed:philadelphia-mbos:necdna","regionalfed:philadelphia-mbos:necdsa","regionalfed:philadelphia-mbos:necina","regionalfed:philadelphia-mbos:necisa","regionalfed:philadelphia-mbos:necnna","regionalfed:philadelphia-mbos:necnsa","regionalfed:philadelphia-mbos:nefdfna","regionalfed:philadelphia-mbos:nefdfsa","regionalfed:philadelphia-mbos:nefdna","regionalfed:philadelphia-mbos:nefdsa","regionalfed:philadelphia-mbos:nefina","regionalfed:philadelphia-mbos:nefisa","regionalfed:philadelphia-mbos:nefnna","regionalfed:philadelphia-mbos:nefnsa","regionalfed:philadelphia-mbos:nocdfna","regionalfed:philadelphia-mbos:nocdfsa","regionalfed:philadelphia-mbos:nocdna","regionalfed:philadelphia-mbos:nocdsa","regionalfed:philadelphia-mbos:nocina","regionalfed:philadelphia-mbos:nocisa","regionalfed:philadelphia-mbos:nocnna","regionalfed:philadelphia-mbos:nocnsa","regionalfed:philadelphia-mbos:nofdfna","regionalfed:philadelphia-mbos:nofdfsa","regionalfed:philadelphia-mbos:nofdna","regionalfed:philadelphia-mbos:nofdsa","regionalfed:philadelphia-mbos:nofina","regionalfed:philadelphia-mbos:nofisa","regionalfed:philadelphia-mbos:nofnna","regionalfed:philadelphia-mbos:nofnsa","regionalfed:philadelphia-mbos:ppcdfna","regionalfed:philadelphia-mbos:ppcdfsa","regionalfed:philadelphia-mbos:ppcdna","regionalfed:philadelphia-mbos:ppcdsa","regionalfed:philadelphia-mbos:ppcina","regionalfed:philadelphia-mbos:ppcisa","regionalfed:philadelphia-mbos:ppcnna","regionalfed:philadelphia-mbos:ppcnsa","regionalfed:philadelphia-mbos:ppfdfna","regionalfed:philadelphia-mbos:ppfdfsa","regionalfed:philadelphia-mbos:ppfdna","regionalfed:philadelphia-mbos:ppfdsa","regionalfed:philadelphia-mbos:ppfina","regionalfed:philadelphia-mbos:ppfisa","regionalfed:philadelphia-mbos:ppfnna","regionalfed:philadelphia-mbos:ppfnsa","regionalfed:philadelphia-mbos:prcdfna","regionalfed:philadelphia-mbos:prcdfsa","regionalfed:philadelphia-mbos:prcdna","regionalfed:philadelphia-mbos:prcdsa","regionalfed:philadelphia-mbos:prcina","regionalfed:philadelphia-mbos:prcisa","regionalfed:philadelphia-mbos:prcnna","regionalfed:philadelphia-mbos:prcnsa","regionalfed:philadelphia-mbos:prfdfna","regionalfed:philadelphia-mbos:prfdfsa","regionalfed:philadelphia-mbos:prfdna","regionalfed:philadelphia-mbos:prfdsa","regionalfed:philadelphia-mbos:prfina","regionalfed:philadelphia-mbos:prfisa","regionalfed:philadelphia-mbos:prfnna","regionalfed:philadelphia-mbos:prfnsa","regionalfed:philadelphia-mbos:shcdfna","regionalfed:philadelphia-mbos:shcdfsa","regionalfed:philadelphia-mbos:shcdna","regionalfed:philadelphia-mbos:shcdsa","regionalfed:philadelphia-mbos:shcina","regionalfed:philadelphia-mbos:shcisa","regionalfed:philadelphia-mbos:shcnna","regionalfed:philadelphia-mbos:shcnsa","regionalfed:philadelphia-mbos:shfdfna","regionalfed:philadelphia-mbos:shfdfsa","regionalfed:philadelphia-mbos:shfdna","regionalfed:philadelphia-mbos:shfdsa","regionalfed:philadelphia-mbos:shfina","regionalfed:philadelphia-mbos:shfisa","regionalfed:philadelphia-mbos:shfnna","regionalfed:philadelphia-mbos:shfnsa","regionalfed:philadelphia-mbos:uocdfna","regionalfed:philadelphia-mbos:uocdfsa","regionalfed:philadelphia-mbos:uocdna","regionalfed:philadelphia-mbos:uocdsa","regionalfed:philadelphia-mbos:uocina","regionalfed:philadelphia-mbos:uocisa","regionalfed:philadelphia-mbos:uocnna","regionalfed:philadelphia-mbos:uocnsa","regionalfed:philadelphia-mbos:uofdfna","regionalfed:philadelphia-mbos:uofdfsa","regionalfed:philadelphia-mbos:uofdna","regionalfed:philadelphia-mbos:uofdsa","regionalfed:philadelphia-mbos:uofina","regionalfed:philadelphia-mbos:uofisa","regionalfed:philadelphia-mbos:uofnna","regionalfed:philadelphia-mbos:uofnsa","regionalfed:richmond-manufacturing:nsa_mfg_bk_logs_c","regionalfed:richmond-manufacturing:nsa_mfg_bk_logs_e","regionalfed:richmond-manufacturing:nsa_mfg_bus_svcs_expnd_c","regionalfed:richmond-manufacturing:nsa_mfg_bus_svcs_expnd_e","regionalfed:richmond-manufacturing:nsa_mfg_cap_util_c","regionalfed:richmond-manufacturing:nsa_mfg_cap_util_e","regionalfed:richmond-manufacturing:nsa_mfg_capital_expnd_c","regionalfed:richmond-manufacturing:nsa_mfg_capital_expnd_e","regionalfed:richmond-manufacturing:nsa_mfg_emp_c","regionalfed:richmond-manufacturing:nsa_mfg_equip_sftw_expnd_c","regionalfed:richmond-manufacturing:nsa_mfg_equip_sftw_expnd_e","regionalfed:richmond-manufacturing:nsa_mfg_fd_gds_inv_c","regionalfed:richmond-manufacturing:nsa_mfg_fd_gds_inv_e","regionalfed:richmond-manufacturing:nsa_mfg_local_bus_cond_c","regionalfed:richmond-manufacturing:nsa_mfg_local_bus_cond_e","regionalfed:richmond-manufacturing:nsa_mfg_nec_skls_avail_c","regionalfed:richmond-manufacturing:nsa_mfg_nec_skls_avail_e","regionalfed:richmond-manufacturing:nsa_mfg_new_orders_c","regionalfed:richmond-manufacturing:nsa_mfg_new_orders_e","regionalfed:richmond-manufacturing:nsa_mfg_pct_chg_prcs_pd_c","regionalfed:richmond-manufacturing:nsa_mfg_pct_chg_prcs_pd_e","regionalfed:richmond-manufacturing:nsa_mfg_pct_chg_prcs_recd_c","regionalfed:richmond-manufacturing:nsa_mfg_pct_chg_prcs_recd_e","regionalfed:richmond-manufacturing:nsa_mfg_raw_mats_inv_c","regionalfed:richmond-manufacturing:nsa_mfg_raw_mats_inv_e","regionalfed:richmond-manufacturing:nsa_mfg_ship_c","regionalfed:richmond-manufacturing:nsa_mfg_ship_e","regionalfed:richmond-manufacturing:nsa_mfg_vend_lead_c","regionalfed:richmond-manufacturing:nsa_mfg_vend_lead_e","regionalfed:richmond-manufacturing:nsa_mfg_wage_c","regionalfed:richmond-manufacturing:nsa_mfg_wage_e","regionalfed:richmond-manufacturing:nsa_mfg_workwk_c","regionalfed:richmond-manufacturing:nsa_mfg_workwk_e","regionalfed:richmond-manufacturing:sa_mfg_bk_logs_c","regionalfed:richmond-manufacturing:sa_mfg_bk_logs_e","regionalfed:richmond-manufacturing:sa_mfg_bus_svcs_expnd_c","regionalfed:richmond-manufacturing:sa_mfg_bus_svcs_expnd_e","regionalfed:richmond-manufacturing:sa_mfg_cap_util_c","regionalfed:richmond-manufacturing:sa_mfg_cap_util_e","regionalfed:richmond-manufacturing:sa_mfg_capital_expnd_c","regionalfed:richmond-manufacturing:sa_mfg_capital_expnd_e","regionalfed:richmond-manufacturing:sa_mfg_composite","regionalfed:richmond-manufacturing:sa_mfg_emp_c","regionalfed:richmond-manufacturing:sa_mfg_equip_sftw_expnd_c","regionalfed:richmond-manufacturing:sa_mfg_equip_sftw_expnd_e","regionalfed:richmond-manufacturing:sa_mfg_fd_gds_inv_c","regionalfed:richmond-manufacturing:sa_mfg_fd_gds_inv_e","regionalfed:richmond-manufacturing:sa_mfg_local_bus_cond_c","regionalfed:richmond-manufacturing:sa_mfg_local_bus_cond_e","regionalfed:richmond-manufacturing:sa_mfg_nec_skls_avail_c","regionalfed:richmond-manufacturing:sa_mfg_nec_skls_avail_e","regionalfed:richmond-manufacturing:sa_mfg_new_orders_c","regionalfed:richmond-manufacturing:sa_mfg_new_orders_e","regionalfed:richmond-manufacturing:sa_mfg_raw_mats_inv_c","regionalfed:richmond-manufacturing:sa_mfg_raw_mats_inv_e","regionalfed:richmond-manufacturing:sa_mfg_ship_c","regionalfed:richmond-manufacturing:sa_mfg_ship_e","regionalfed:richmond-manufacturing:sa_mfg_vend_lead_c","regionalfed:richmond-manufacturing:sa_mfg_vend_lead_e","regionalfed:richmond-manufacturing:sa_mfg_wage_c","regionalfed:richmond-manufacturing:sa_mfg_wage_e","regionalfed:richmond-manufacturing:sa_mfg_workwk_c","regionalfed:richmond-manufacturing:sa_mfg_workwk_e","regionalfed:richmond-nonmanufacturing:nsa_svc_ave_wage_c","regionalfed:richmond-nonmanufacturing:nsa_svc_ave_wage_e","regionalfed:richmond-nonmanufacturing:nsa_svc_ave_workwk_c","regionalfed:richmond-nonmanufacturing:nsa_svc_ave_workwk_e","regionalfed:richmond-nonmanufacturing:nsa_svc_bus_svcs_expnd_c","regionalfed:richmond-nonmanufacturing:nsa_svc_bus_svcs_expnd_e","regionalfed:richmond-nonmanufacturing:nsa_svc_capital_expnd_c","regionalfed:richmond-nonmanufacturing:nsa_svc_capital_expnd_e","regionalfed:richmond-nonmanufacturing:nsa_svc_demand_c","regionalfed:richmond-nonmanufacturing:nsa_svc_demand_e","regionalfed:richmond-nonmanufacturing:nsa_svc_emp_c","regionalfed:richmond-nonmanufacturing:nsa_svc_emp_e","regionalfed:richmond-nonmanufacturing:nsa_svc_equip_sftw_expnd_c","regionalfed:richmond-nonmanufacturing:nsa_svc_equip_sftw_expnd_e","regionalfed:richmond-nonmanufacturing:nsa_svc_local_bus_cond_c","regionalfed:richmond-nonmanufacturing:nsa_svc_local_bus_cond_e","regionalfed:richmond-nonmanufacturing:nsa_svc_nec_skls_avail_c","regionalfed:richmond-nonmanufacturing:nsa_svc_nec_skls_avail_e","regionalfed:richmond-nonmanufacturing:nsa_svc_pct_chg_prcs_pd_c","regionalfed:richmond-nonmanufacturing:nsa_svc_pct_chg_prcs_pd_e","regionalfed:richmond-nonmanufacturing:nsa_svc_prcs_recd_c","regionalfed:richmond-nonmanufacturing:nsa_svc_prcs_recd_e","regionalfed:richmond-nonmanufacturing:nsa_svc_revs_sales_c","regionalfed:richmond-nonmanufacturing:nsa_svc_revs_sales_e","regionalfed:richmond-nonmanufacturing:sa_svc_ave_wage_c","regionalfed:richmond-nonmanufacturing:sa_svc_ave_wage_e","regionalfed:richmond-nonmanufacturing:sa_svc_ave_workwk_c","regionalfed:richmond-nonmanufacturing:sa_svc_ave_workwk_e","regionalfed:richmond-nonmanufacturing:sa_svc_bus_svcs_expnd_c","regionalfed:richmond-nonmanufacturing:sa_svc_bus_svcs_expnd_e","regionalfed:richmond-nonmanufacturing:sa_svc_capital_expnd_c","regionalfed:richmond-nonmanufacturing:sa_svc_capital_expnd_e","regionalfed:richmond-nonmanufacturing:sa_svc_demand_c","regionalfed:richmond-nonmanufacturing:sa_svc_demand_e","regionalfed:richmond-nonmanufacturing:sa_svc_emp_c","regionalfed:richmond-nonmanufacturing:sa_svc_emp_e","regionalfed:richmond-nonmanufacturing:sa_svc_equip_sftw_expnd_c","regionalfed:richmond-nonmanufacturing:sa_svc_equip_sftw_expnd_e","regionalfed:richmond-nonmanufacturing:sa_svc_local_bus_cond_c","regionalfed:richmond-nonmanufacturing:sa_svc_local_bus_cond_e","regionalfed:richmond-nonmanufacturing:sa_svc_nec_skls_avail_c","regionalfed:richmond-nonmanufacturing:sa_svc_nec_skls_avail_e","regionalfed:richmond-nonmanufacturing:sa_svc_revs_sales_c","regionalfed:richmond-nonmanufacturing:sa_svc_revs_sales_e"].map(function(id){return [id.toLowerCase(),id];}));
  var SERIES_PROV = { regionalfed:1, defillama:1, ustpar:1, fred:1, calc:1, nyfed:1, eurostat:1, ecb:1, oecd:1, bis:1, bisfx:1, imf:1, cboeindex:1, boj:1, statcan:1, worldbank:1, ofr:1, "ofr-fsi":1, "ofr-hfm":1, "ofr-bsrm":1, "ofr-site":1, bls:1, census:1, "census-us":1, bea:1, treasury:1, boe:1, eia:1, te:1, "te-mirror":1, "te-feed":1, chicagofed:1, clevelandfed:1, atlantafed:1, cboe:1, cftc:1, dbnomics:1, banxico:1, snb:1, bcb:1, "official-yields":1, tic:1, "kr-ecos":1, "taiwan-moea":1, "peru-copper":1, "cl-datos":1, "hk-data":1, nasa:1, occ:1, dol:1, finra:1, eiopa:1, gleif:1, gdelt:1, "fed-board":1, cryptoquant:1, coinmetrics:1, fmp:1, quiver:1, benzinga:1, "indicator-bus":1, "nyfed-research":1, "sec-edgar":1, "sec-midas":1, "sec-dera":1, "sec-bulk":1 };
  var CHIPS = [
    ["all", "All"],
    ["stocks", "Stocks"],
    ["etfs", "Funds"],
    ["macro", "Macro"],
    ["data", "Datasets"],
    ["flows", "ETF flows"],
    ["inst", "13F"],
    ["chain", "On-chain"],
    ["bonds", "Bonds"],
    ["stress", "Stress"],
    ["energy", "Energy"],
    ["trade", "Trade"],
    ["crypto", "Crypto"],
    ["fx", "Forex"],
    ["lists", "Lists"],
    ["notes", "Notes"]
  ];
  var TAB_CLS = {
    stocks: { stock: 1 },
    etfs: { etf: 1, fund: 1, flows: 1 },
    crypto: { crypto: 1, onchain: 1, chain: 1 },
    fx: { fx: 1, forex: 1 },
    macro: { macro: 1, economy: 1 },
    flows: { flows: 1, etf: 1, fund: 1 },
    inst: { inst: 1, desk: 1, ownership: 1 },
    chain: { onchain: 1, chain: 1, crypto: 1 },
    bonds: { bonds: 1, bond: 1, credit: 1, economy: 1, macro: 1 },
    stress: { stress: 1, liquidity: 1, macro: 1 },
    energy: { energy: 1, commodity: 1 },
    trade: { trade: 1, ports: 1, exports: 1, macro: 1 },
    lists: {},
    notes: {},
    data: { dataset: 1 },
    all: {}
  };

  function H(q, s, name, cat, extra, type) {
    return { q: q, s: s, name: name, cat: cat, extra: extra, type: type || cat };
  }

  /* Curated, chartable. extra is the honest history / cadence label. */
  var CURATED = [
    H(["sofr", "secured overnight"], "FRED:SOFR", "SOFR overnight rate", "macro", "FRED daily · warehouse history from 2018-04-03", "economy"),
    H(["effr", "effective federal funds", "fed funds", "dff", "fedfunds"], "FRED:DFF", "Effective Federal Funds Rate", "macro", "FRED daily · full history", "economy"),
    H(["us10y", "10y", "10 year", "dgs10", "treasury 10y"], "FRED:DGS10", "US 10-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["us2y", "2y", "dgs2"], "FRED:DGS2", "US 2-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["us5y", "5y", "dgs5"], "FRED:DGS5", "US 5-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["us30y", "30y", "dgs30"], "FRED:DGS30", "US 30-Year Treasury yield", "macro", "FRED daily · full history", "economy"),
    H(["t10y2y", "2s10s", "yield curve"], "FRED:T10Y2Y", "10Y–2Y Treasury spread", "macro", "FRED daily · full history", "economy"),
    H(["t10y3m", "3m10y"], "FRED:T10Y3M", "10Y–3M Treasury spread", "macro", "FRED daily · full history", "economy"),
    H(["tips 10y", "dfii10", "real yield"], "FRED:DFII10", "10Y TIPS real yield", "macro", "FRED daily · full history", "economy"),
    H(["breakeven", "t10yie", "inflation breakeven"], "FRED:T10YIE", "10Y inflation breakeven", "macro", "FRED daily · full history", "economy"),
    H(["walcl", "fed balance sheet"], "FRED:WALCL", "Fed balance sheet (WALCL)", "macro", "FRED weekly · full history", "economy"),
    H(["rrp", "reverse repo", "rrpontsyd"], "FRED:RRPONTSYD", "ON RRP facility", "macro", "FRED daily · full history", "economy"),
    H(["reserve balances", "wresbal"], "FRED:WRESBAL", "Reserve balances", "macro", "FRED weekly · full history", "economy"),
    H(["cpi", "inflation"], "FRED:CPIAUCSL", "US CPI", "macro", "FRED monthly · full history", "economy"),
    H(["unrate", "unemployment"], "FRED:UNRATE", "Unemployment rate", "macro", "FRED monthly · full history since 1948", "economy"),
    H(["nfp", "payrolls", "payems"], "FRED:PAYEMS", "Nonfarm payrolls", "macro", "FRED monthly · full history", "economy"),
    H(["jobless", "icsa", "claims"], "FRED:ICSA", "Initial jobless claims", "macro", "FRED weekly · full history", "economy"),
    H(["m2", "m2sl"], "FRED:M2SL", "M2 money stock", "macro", "FRED monthly · full history", "economy"),
    H(["nfci", "chicago fed"], "FRED:NFCI", "Chicago Fed NFCI", "stress", "FRED weekly · full history", "stress"),
    H(["hy oas", "high yield spread", "bamlh0a0hym2"], "FRED:BAMLH0A0HYM2", "ICE BofA HY OAS", "bonds", "FRED daily · full history", "bonds"),
    H(["ig oas", "bamlc0a0cm"], "FRED:BAMLC0A0CM", "ICE BofA IG OAS", "bonds", "FRED daily · full history", "bonds"),
    H(["move", "move index"], "FRED:MOVE", "MOVE bond vol", "bonds", "FRED daily when present", "bonds"),
    H(["vixcls", "vix close"], "FRED:VIXCLS", "VIX close (FRED)", "stress", "FRED daily · full history", "stress"),
    H(["dollar", "dxy", "dtwexbgs", "broad dollar"], "FRED:DTWEXBGS", "Trade-weighted USD (broad)", "macro", "FRED daily · full history", "economy"),
    H(["wti", "oil", "dcoilwtico"], "FRED:DCOILWTICO", "WTI spot (FRED)", "energy", "FRED daily · full history", "energy"),
    H(["exports", "expgs", "us exports"], "FRED:EXPGS", "US goods & services exports", "trade", "FRED monthly · full history", "trade"),
    H(["imports", "impgs", "us imports"], "FRED:IMPGS", "US goods & services imports", "trade", "FRED monthly · full history", "trade"),
    H(["trade balance", "bopgstb", "bop"], "FRED:BOPGSTB", "US trade balance", "trade", "FRED monthly · full history", "trade"),
    H(["gdp", "gdpc1"], "FRED:GDPC1", "Real GDP", "macro", "FRED quarterly · full history", "economy"),
    H(["indpro", "industrial production"], "FRED:INDPRO", "Industrial production", "macro", "FRED monthly · full history", "economy"),
    H(["housing starts", "houst"], "FRED:HOUST", "Housing starts", "macro", "FRED monthly · full history", "economy"),
    H(["retail sales", "rsafs"], "FRED:RSAFS", "Advance retail sales", "macro", "FRED monthly · full history", "economy"),
    H(["sp500 fred"], "FRED:SP500", "S&P 500 (FRED)", "macro", "FRED daily · full history", "economy"),

    H(["ciss", "euro stress", "sovereign stress"], "CISS:ea", "EA CISS composite", "stress", "ECB CISS · harvest history from 2000", "stress"),

    H(["13f", "13-f", "institutional ownership", "smart money 13f"], "DESK:inst", "13F institutional book", "inst", "SEC 13F · quarterly lagged snapshot", "desk"),
    H(["institutional volume", "inst vol", "dark pool", "ats volume", "finra ats"], "DESK:ivol", "Institutional volume (ATS + tape + 13F)", "inst", "FINRA weekly ATS + Polygon week + 13F confirmation — never blended", "desk"),
    H(["etf holdings", "holdings book", "constituents"], "DESK:etf", "ETF holdings + vs-SPX ranks", "flows", "Massive ETF Global + holdings-index", "desk"),
    H(["etf flow", "creations", "redemptions", "inflow", "outflow"], "DESK:flow", "ETF creations / redemptions", "flows", "Massive ETF Global · delayed tape", "desk"),
    H(["valuation", "pe ratio", "ttm ratios"], "DESK:val", "Valuation (FMP EOD/TTM)", "stocks", "FMP Ultimate · TTM snapshot, not a live print", "desk"),
    H(["financials", "income statement", "filings"], "DESK:fin", "Financials (FMP filings)", "stocks", "FMP Ultimate · annual/quarterly filings", "desk"),
    H(["identity", "figi", "openfigi", "cusip"], "DESK:ident", "Identity / OpenFIGI", "stocks", "OpenFIGI symbology master", "desk"),
    H(["on-chain desk", "onchain desk"], "DESK:chain", "On-chain desk", "chain", "CryptoQuant EOD", "desk"),
    H(["pressure", "buying pressure"], "DESK:press", "Buying / selling pressure", "flows", "ETF look-through · inferred", "desk"),
    H(["liquidity pulse", "rrp liquidity"], "FRED:RRPONTSYD", "ON RRP (liquidity pulse)", "stress", "FRED daily · full history", "stress")
  ];

  /* Every harvest series in /data/cryptoquant-series.json. extra is honest:
   * Exact primary history only; matching proxy histories remain separate. */
  var CQ_META = [
    ["btc_exchange_netflow", "BTC exchange netflow", ["netflow", "exchange netflow", "btc netflow"]],
    ["btc_exchange_inflow", "BTC exchange inflow", ["exchange inflow", "inflow"]],
    ["btc_exchange_outflow", "BTC exchange outflow", ["exchange outflow", "outflow"]],
    ["btc_exchange_reserve", "BTC exchange reserve", ["exchange reserve", "exchange balance"]],
    ["btc_exchange_addr_in", "BTC exchange depositing addresses", ["depositing addresses", "exchange addresses"]],
    ["btc_mpi", "BTC miner position index", ["mpi", "miner position", "miners position"]],
    ["btc_whale_ratio", "BTC whale ratio", ["whale ratio", "whale"]],
    ["btc_fund_flow_ratio", "BTC fund flow ratio", ["fund flow ratio", "fund flow"]],
    ["btc_stablecoins_ratio", "BTC stablecoins ratio", ["stablecoins ratio"]],
    ["btc_exchange_supply_ratio", "BTC exchange supply ratio", ["exchange supply ratio"]],
    ["btc_mvrv", "BTC MVRV", ["mvrv", "btc mvrv"]],
    ["btc_sopr", "BTC SOPR", ["sopr", "btc sopr"]],
    ["btc_sopr_ratio", "BTC SOPR ratio", ["sopr ratio"]],
    ["btc_nupl", "BTC NUPL", ["nupl"]],
    ["btc_realized_price", "BTC realized price", ["realized price"]],
    ["btc_ssr", "BTC SSR", ["ssr", "stablecoin supply ratio"]],
    ["btc_nvt", "BTC NVT", ["nvt"]],
    ["btc_nvt_golden", "BTC NVT golden", ["nvt golden"]],
    ["btc_nvm", "BTC NVM", ["nvm"]],
    ["btc_puell", "BTC Puell multiple", ["puell", "puell multiple"]],
    ["btc_stock_to_flow", "BTC stock-to-flow", ["stock to flow", "s2f", "stock-to-flow"]],
    ["btc_miner_netflow", "BTC miner netflow", ["miner netflow"]],
    ["btc_miner_outflow", "BTC miner outflow", ["miner outflow"]],
    ["btc_miner_reserve", "BTC miner reserve", ["miner reserve"]],
    ["btc_tx_count", "BTC transaction count", ["tx count", "transaction count"]],
    ["btc_addresses_active", "BTC active addresses", ["active addresses", "addresses active"]],
    ["btc_fees_total", "BTC fees · definition conflict", ["fees total", "btc fees", "fees block mean", "definition conflict"]],
    ["btc_fees_tx_mean", "BTC mean fee", ["mean fee", "fee per tx"]],
    ["btc_blockreward", "BTC block reward", ["block reward"]],
    ["btc_difficulty", "BTC difficulty", ["difficulty"]],
    ["btc_hashrate", "BTC hashrate", ["hashrate", "hash rate"]],
    ["btc_utxo_count", "BTC UTXO count", ["utxo"]],
    ["btc_velocity", "BTC velocity", ["velocity"]],
    ["btc_tokens_transferred", "BTC tokens transferred", ["tokens transferred"]],
    ["btc_supply_total", "BTC supply", ["btc supply", "circulating supply"]],
    ["btc_open_interest", "BTC open interest", ["open interest", "oi"]],
    ["btc_funding_rates", "BTC funding rates", ["funding", "funding rates"]],
    ["btc_liquidations", "BTC liquidations", ["liquidations"]],
    ["btc_taker_ratio", "BTC taker buy ratio", ["taker ratio", "taker buy"]],
    ["btc_coinbase_premium", "BTC Coinbase premium", ["coinbase premium"]],
    ["eth_exchange_netflow", "ETH exchange netflow", ["eth netflow"]],
    ["eth_exchange_inflow", "ETH exchange inflow", ["eth inflow"]],
    ["eth_exchange_outflow", "ETH exchange outflow", ["eth outflow"]],
    ["eth_exchange_reserve", "ETH exchange reserve", ["eth reserve"]],
    ["eth_addresses_active", "ETH active addresses", ["eth addresses"]],
    ["eth_tx_count", "ETH transaction count", ["eth tx"]],
    ["eth_open_interest", "ETH open interest", ["eth oi", "eth open interest"]],
    ["eth_funding_rates", "ETH funding rates", ["eth funding"]],
    ["eth_mvrv", "ETH MVRV", ["eth mvrv"]],
    ["stablecoin_exchange_reserve", "Stablecoin exchange reserve", ["stablecoin reserve"]],
    ["stablecoin_exchange_netflow", "Stablecoin exchange netflow", ["stablecoin netflow"]],
    ["stablecoin_exchange_inflow", "Stablecoin exchange inflow", ["stablecoin inflow"]],
    ["stablecoin_exchange_outflow", "Stablecoin exchange outflow", ["stablecoin outflow"]],
    ["stablecoin_supply_total", "Stablecoin supply", ["stablecoin supply"]],
    ["usdc_exchange_reserve", "USDC exchange reserve", ["usdc reserve"]]
  ];
  var CQ_COLORS = ["#f0b429", "#2962ff", "#26c6da", "#ab47bc", "#ff6d00", "#089981", "#f23645", "#7e57c2", "#00897b", "#e91e63"];
  CQ_META.forEach(function (row) {
    var q = row[2].concat([row[0], row[0].replace(/_/g, " ")]);
    CURATED.push(H(q, "CQ:" + row[0], row[1], "chain", "CryptoQuant reported observations · source freshness unverified · proxies separate", "onchain"));
  });
  function cqOscSpecs() {
    var rows = CQ_META;
    if (global.JHCqFuse && typeof global.JHCqFuse.seriesMeta === "function") {
      var live = global.JHCqFuse.seriesMeta();
      if (live && live.length >= 8) {
        rows = live.map(function (m) { return [m.id, m.name, m.q || []]; });
      }
    }
    return rows.map(function (row, i) {
      return {
        id: "cq_" + row[0],
        n: row[1],
        on: 0,
        cat: "On-chain",
        c: CQ_COLORS[i % CQ_COLORS.length],
        k: "cq",
        cq: row[0]
      };
    });
  }

  function norm(q) {
    return String(q || "").toLowerCase().replace(/[^a-z0-9:+.\- ]+/g, " ").replace(/\s+/g, " ").trim();
  }

  function chips() { return CHIPS; }

  function isWarehouse(s) {
    s = String(s || "");
    if (/^COT[23]?:/i.test(s)) return true;
    if (/^(FRED|CQ|CISS|DESK|DATA|NYFED):/i.test(s)) return true;
    var p = s.split(":")[0];
    if (!p || s.indexOf(":") < 0) return false;
    if (EXCH[p.toUpperCase()]) return false;
    return Object.prototype.hasOwnProperty.call(SERIES_PROV,p.toLowerCase());
  }

  function tabMatch(tab, cls, type, s) {
    if (!tab || tab === "all") return true;
    if (tab === "lists" || tab === "notes") return true;
    var want = TAB_CLS[tab];
    if (!want || !Object.keys(want).length) return true;
    var c = String(cls || type || "").toLowerCase();
    if (want[c]) return true;
    var hit = CURATED.filter(function (x) { return x.s === s; })[0];
    if (hit && (hit.cat === tab || want[hit.cat] || want[hit.type])) return true;
    if (tab === "macro" && /^FRED:/i.test(s)) return true;
    if (tab === "macro" && (isWarehouse(s) || type === "economy" || c === "macro")) return true;
    if (tab === "data") return type === "dataset" || /^provider:/i.test(s) || c === "dataset";
    if (tab === "chain" && /^CQ:|^CQSNAP:|^CQARM:|^CQDOC:/i.test(s)) return true;
    if (tab === "stress" && /^CISS:/i.test(s)) return true;
    if (tab === "inst" && /^DESK:inst/i.test(s)) return true;
    return false;
  }

  function aliasHits(q) {
    var n = norm(q);
    if (!n) return [];
    var out = [], seen = {};
    CURATED.forEach(function (a) {
      var hit = a.q.some(function (k) { return k === n || n.indexOf(k) >= 0 || k.indexOf(n) >= 0; });
      if (hit && !seen[a.s]) {
        seen[a.s] = 1;
        out.push({ s: a.s, name: a.name, extra: a.extra, type: a.type, cat: a.cat, suggest: true });
      }
    });
    return out;
  }

  function search(q) {
    return suggest(q, 24);
  }

  function keepId(s) {
    s = String(s || "");
    if (isWarehouse(s) || /^(FRED|CQ|CISS|DESK|DATA|NYFED|CQSNAP|CQARM|CQDOC):/i.test(s)) return s;
    if (/^\^/.test(s) || s.indexOf("=") >= 0) return s.toUpperCase();
    if (/^(NASDAQ|NYSE|AMEX|ARCA|CBOE|TVC|BINANCE):/i.test(s)) return s;
    return s;
  }

  function scoreBlob(q, ticker, name, blob) {
    var n = q.toLowerCase();
    var t = String(ticker || "").toLowerCase();
    var nm = String(name || "").toLowerCase();
    var b = String(blob || "").toLowerCase();
    if (t === n) return 100;
    if (t.indexOf(n) === 0) return 92;
    if (nm === n) return 88;
    if (nm.indexOf(n) === 0) return 80;
    if (b.indexOf(n) === 0) return 78;
    if (nm.indexOf(n) >= 0) return 64;
    if (b.indexOf(n) >= 0) return 55;
    return 0;
  }

  function suggest(q, limit) {
    limit = limit || 16;
    var n = norm(q);
    var out = aliasHits(q);
    var seen = {};
    var i, row, sc;
    out.forEach(function (h) { seen[String(h.s).toUpperCase()] = 1; });
    function push(hit, score) {
      var k = String(hit.s).toUpperCase();
      if (!hit.s || seen[k]) return;
      seen[k] = 1;
      hit.suggest = true;
      hit.score = score;
      out.push(hit);
    }
    if (!n) return out.slice(0, limit);
    if (/cq|on.?chain|cryptoquant|mvrv|sopr|nupl|hashrate|puell|a_sopr|in-house|block.interval|cdd|dormancy|eth2|lightning|mvrv.?z|xrp|trx|stablecoin|mempool|miner|realized|exchange.?flow|whale|ssr|nvt|coin.?day|apparent.?demand|etf.?demand|utxo|hodl|asopr|mpi|netflow|reserve/i.test(n)) limit = Math.max(limit, 80);
    if (global.JHCqFuse && typeof global.JHCqFuse.searchHits === "function" && global.JHCqFuse.pack()) {
      if (/cq|on.?chain|cryptoquant|cdd|dormancy|eth2|lightning|xrp|trx|mvrv.?z/i.test(n)) limit = Math.max(limit, 400);
      global.JHCqFuse.searchHits(n, limit).forEach(function (h) { push(h, h.score || 85); });
    }
    if (SYM) {
      var i, row, sc;
      for (i = 0; i < SYM.length; i++) {
        row = SYM[i];
        sc = scoreBlob(n, row.s, row.n, row.blob);
        if (n.length === 1 && sc < 90) continue;
        if (sc) push({ s: row.s, name: row.n, extra: row.ids, type: "stock", cat: "stocks" }, sc);
      }
    }
    if (IND && n.length >= 2) {
      for (i = 0; i < IND.length; i++) {
        row = IND[i];
        if (row.s.toLowerCase().indexOf(n) !== 0 && (row.n || "").toLowerCase().indexOf(n) !== 0) continue;
        push({ s: row.chart, name: row.n || row.s, extra: row.extra, type: row.type, cat: row.cat }, 70);
      }
    }
    if (CQ && CQ.series && n.length >= 2) {
      var cqWant = n.replace(/^cq:/, "").replace(/\s+/g, "_");
      Object.keys(CQ.series).forEach(function (id) {
        var blob = id.replace(/_/g, " ");
        if (id.toLowerCase().indexOf(cqWant) < 0 && blob.indexOf(n) < 0) return;
        var meta = CQ_META.filter(function (r) { return r[0] === id; })[0];
        push({ s: "CQ:" + id, name: meta ? meta[1] : id, extra: "CryptoQuant EOD · harvest", type: "onchain", cat: "chain" }, 85);
      });
    }
    if (INST && n.length >= 1) {
      var hits = [];
      for (i = 0; i < INST.length; i++) {
        row = INST[i];
        sc = 0;
        if (row.u === n.toUpperCase()) sc = 100;
        else if (row.u.indexOf(n.toUpperCase()) === 0) sc = 90;
        else if (n.length >= 3 && row.nu.indexOf(n.toUpperCase()) === 0) sc = 70;
        else if (n.length >= 4 && row.nu.indexOf(n.toUpperCase()) >= 0) sc = 50;
        if (!sc) continue;
        sc += (row.pop || 0) * 8;
        hits.push({ sc: sc, row: row });
      }
      hits.sort(function (a, b) { return b.sc - a.sc; });
      for (i = 0; i < hits.length && i < 8; i++) {
        row = hits[i].row;
        push({ s: row.s, name: row.n, extra: (row.ex || row.m || "") + " " + (row.t || "symbol"), type: (row.t || "stock").toLowerCase(), cat: "stocks" }, hits[i].sc);
      }
    }
    if (PROV && n.length >= 2) {
      for (i = 0; i < PROV.length; i++) {
        row = PROV[i];
        if (row.blob.indexOf(n) < 0) continue;
        push({ s: "provider:" + row.slug, name: row.name, extra: (row.n ? Number(row.n).toLocaleString() + " datasets · " : "") + "warehouse provider", type: "dataset", cat: "data" }, row.slug === n ? 95 : 60);
      }
    }
    out.sort(function (a, b) { return (b.score || 0) - (a.score || 0); });
    return out.slice(0, limit);
  }

  function identifierText(value, pattern) {
    if (typeof value !== "string" || value.length > 128) return "";
    var normalized = value.trim().toUpperCase();
    return pattern.test(normalized) ? normalized : "";
  }

  function lookupSym(q) {
    if (!SYM || !q) return "";
    var n = String(q).trim().toUpperCase(), matches = [];
    // The source has no independently qualified security-identifier relationships.
    // Preserve exact ticker lookup; an identifier candidate is never a shortcut.
    for (var i = 0; i < SYM.length; i++) {
      if (SYM[i].s.toUpperCase() === n) matches.push(SYM[i].s);
    }
    return matches.length === 1 ? matches[0] : "";
  }

  function catalogPopulation(key, doc) {
    var value, master=key==="master"?doc:null, bus=key==="bus"?doc:null;
    var catalog=key==="providers"?doc:null, instr=key==="instruments"?doc:null;
    function object(x) { return x && typeof x==="object" && !Array.isArray(x); }
    if (!object(doc)) throw new Error("invalid_catalog_object");
    if (key==="master" && !object(doc.by_ticker)) throw new Error("invalid_ticker_population");
    if (key==="bus" && !object(doc.indicators)) throw new Error("invalid_indicator_population");
    if (key==="providers" && (!Array.isArray(doc.providers) || doc.providers.some(function(row) {
      return !object(row) || typeof row.slug!=="string" || !row.slug.trim() ||
        (row.name!=null && typeof row.name!=="string");
    }))) throw new Error("invalid_provider_population");
    if (key==="instruments" && (!Array.isArray(doc.rows) || doc.rows.some(function(row) {
      return !Array.isArray(row) || typeof row[0]!=="string" || !row[0].trim() ||
        row.slice(1,5).some(function(v) { return v!=null && typeof v!=="string"; });
    }))) throw new Error("invalid_instrument_population");
    if (key==="cq") {
      if (!object(doc.series)) throw new Error("invalid_onchain_population");
      return doc;
    }
    if (key==="fuse") return doc;
      if (master && master.by_ticker && typeof master.by_ticker === "object" && !Array.isArray(master.by_ticker)) {
        value = [];
        Object.keys(master.by_ticker).forEach(function (t) {
          var r = master.by_ticker[t];
          if (!r || typeof r !== "object" || Array.isArray(r)) r = {};
          var name = typeof r.name === "string" && r.name ? r.name :
            typeof r.figi_name === "string" && r.figi_name ? r.figi_name : t;
          var figi = identifierText(r.figi, /^[B-DF-HJ-NP-TV-Z]{2}G[B-DF-HJ-NP-TV-Z0-9]{8}[0-9]$/);
          var cusip = identifierText(r.cusip, /^[A-Z0-9*@#]{8}[0-9]$/);
          var isin = identifierText(r.isin, /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/);
          var ids = [];
          if (figi) ids.push("FIGI " + figi);
          if (cusip) ids.push("CUSIP " + cusip);
          if (isin) ids.push("ISIN " + isin);
          if (ids.length) ids.unshift("Unverified identifier candidates");
          ids.push("SEC ticker source · security relationship unverified");
          value.push({
            s: t,
            n: name,
            figi: figi,
            cusip: cusip,
            isin: isin,
            ids: ids.join(" · "),
            blob: (t + " " + name + " " + figi + " " + cusip + " " + isin).toLowerCase()
          });
        });
      }
      if (bus && bus.indicators) {
        value = [];
        Object.keys(bus.indicators).forEach(function (k) {
          var row = bus.indicators[k] || {};
          var src = String(row.src || "");
          var chart = k, type = "economy", cat = "macro", extra = "indicator-bus";
          if (/^cryptoquant:/i.test(src) || /mvrv|sopr|nupl|netflow/i.test(k)) {
            chart = "CQ:" + k.replace(/^BTC_?/i, "btc_").toLowerCase();
            type = "onchain"; cat = "chain"; extra = "CryptoQuant EOD";
          } else if (/^yahoo:/i.test(src)) {
            chart = src.split(":").slice(1).join(":");
            type = "index"; cat = "macro"; extra = "Yahoo · " + (row.asof || "");
          } else if (/^[A-Z0-9][A-Z0-9._]{1,15}$/.test(k) && !/!/.test(k)) {
            chart = "FRED:" + k;
            extra = "FRED warehouse · full history when present";
          }
          value.push({ s: k, n: k, chart: chart, extra: extra, type: type, cat: cat });
        });
      }
      if (catalog && Array.isArray(catalog.providers)) {
        value = catalog.providers.map(function (p) {
          return {
            slug: p.slug,
            name: p.name || p.slug,
            n: p.datasets || p.n_keys || 0,
            blob: String(p.slug + " " + (p.name || "")).toLowerCase()
          };
        });
      }
      if (instr && Array.isArray(instr.rows)) {
        value = instr.rows.map(function (r) {
          return {
            s: r[0],
            n: r[1] || "",
            ex: r[2] || "",
            t: r[3] || "",
            m: r[4] || "",
            pop: r[5] || 0,
            u: String(r[0] || "").toUpperCase(),
            nu: String(r[1] || "").toUpperCase()
          };
        });
      }
    return value;
  }

  // Per-source download checks. None of these clocks qualifies observation freshness.
  var INDEX_STATE = {}, INDEX_REVISION=0, INDEX_INTERVAL_MS = 300000, INDEX_RETRY_MS = 30000;
  function catalogClock() {
    return {wall:Date.now(), mono:global.performance && typeof global.performance.now==="function" ? global.performance.now() : Date.now()};
  }
  function catalogAge(now, then) {
    if (!then || now.wall<then.wall || now.mono<then.mono) return null;
    var age=Math.max(now.wall-then.wall,now.mono-then.mono);
    return isFinite(age) ? age : null;
  }
  function catalogTimed(job) {
    return new Promise(function(resolve,reject) {
      var controller=typeof global.AbortController==="function" ? new global.AbortController() : null;
      var settled=false, timer=global.setTimeout(function() {
        if (settled) return; settled=true;
        if (controller) controller.abort();
        reject(new Error("catalog_timeout"));
      },10000);
      Promise.resolve().then(function() { return job(controller && controller.signal); }).then(function(value) {
        if (settled) return; settled=true;global.clearTimeout(timer);resolve(value);
      },function(error) {
        if (settled) return; settled=true;global.clearTimeout(timer);reject(error);
      });
    });
  }
  function catalogSpecs() {
    var specs=[
      {key:"master",label:"Security directory",load:function(signal) { return loadJson("/data/symbology/master.json",signal); },put:function(value) { SYM=value; }},
      {key:"bus",label:"Indicator directory",load:function(signal) { return loadJson("/data/indicator-bus.json",signal); },put:function(value) { IND=value; }},
      {key:"providers",label:"Provider directory",load:function(signal) { return loadJson("/data/provider-catalog.json",signal); },put:function(value) { PROV=value; }},
      {key:"instruments",label:"Instrument directory",load:function(signal) {
        return loadJson(PROXY+"/data/symdir/instruments.json.gz",signal).catch(function(error) {
          if (signal && signal.aborted) throw error;
          return loadJson("/data/symdir/instruments.json.gz",signal);
        });
      },put:function(value) { INST=value; }},
      {key:"cq",label:"On-chain catalog",load:function(signal) { return loadJson("/data/cryptoquant-series.json",signal); },put:function(value) { CQ=value; }}
    ];
    if (global.JHCqFuse && typeof global.JHCqFuse.load==="function") {
      specs.push({key:"fuse",label:"On-chain enrichment",load:function() { return global.JHCqFuse.load(); },put:function() {}});
    }
    return specs;
  }
  function indexStatus() {
    var now=catalogClock(), sources=catalogSpecs().map(function(spec) {
      var state=INDEX_STATE[spec.key]||{}, age=catalogAge(now,state.checked), attempt=catalogAge(now,state.attempted);
      var due=age===null || age>=INDEX_INTERVAL_MS;
      var status=state.loading ? "loading" : state.error ? (state.loaded ? "cached" : "unavailable") :
        !state.loaded ? "unavailable" : due ? "cached" : "download_checked";
      // The optional module owns its cache; a returned object proves no new download.
      if(spec.key==="fuse" && state.loaded && !state.loading && !state.error) status="enrichment_unverified";
      return {id:spec.key,label:spec.label,status:status,loaded:!!state.loaded,
        checked_at:spec.key!=="fuse" && state.checked ? new Date(state.checked.wall).toISOString() : null,
        age_s:spec.key==="fuse" || age===null ? null : Math.floor(age/1000),
        retry_after_s:state.error && attempt!==null ? Math.max(0,Math.ceil((INDEX_RETRY_MS-attempt)/1000)) : 0,
        error:state.error||null,source_freshness_verified:false};
    });
    return {revision:INDEX_REVISION,sources:sources,source_freshness_verified:false,refresh_interval_s:INDEX_INTERVAL_MS/1000,
      n_sym:SYM===null?null:SYM.length,n_ind:IND===null?null:IND.length,n_inst:INST===null?null:INST.length,n_prov:PROV===null?null:PROV.length};
  }
  function ensureIndex() {
    if (IDX_P) return IDX_P;
    var jobs=catalogSpecs().map(function(spec) {
      var state=INDEX_STATE[spec.key] || (INDEX_STATE[spec.key]={}), now=catalogClock();
      var age=catalogAge(now,state.attempted), bound=state.error ? INDEX_RETRY_MS : INDEX_INTERVAL_MS;
      if (age!==null && age<bound) return Promise.resolve();
      state.loading=true;state.attempted=now;
      return catalogTimed(spec.load).then(function(doc) {
        var value=catalogPopulation(spec.key,doc);
        spec.put(value);state.loaded=true;state.checked=now;state.error=null;
      }).catch(function() {
        // Preserve the complete previous population, never a partially parsed replacement.
        state.error="download_or_validation_failed";state.attempted=catalogClock();
      }).then(function() { state.loading=false;INDEX_REVISION++; });
    });
    IDX_P=Promise.all(jobs).then(function() { IDX_P=null;return indexStatus(); },function(error) { IDX_P=null;throw error; });
    return IDX_P;
  }

  function classify(s) {
    s = String(s || "");
    if (/^DESK:/i.test(s)) return "desk";
    if (/^DATA:/i.test(s) || /^provider:/i.test(s)) return "dataset";
    if (/^CQ:/i.test(s)) return "onchain";
    if (/^CQSNAP:|^CQARM:|^CQDOC:/i.test(s)) return "onchain";
    if (/^CISS:/i.test(s)) return "stress";
    if (isWarehouse(s)) return "macro";
    return "";
  }

  function chartId(raw) {
    var s = String(raw || "").trim();
    if (!s) return "";
    var low = s.toLowerCase();
    if (/^(COT[23]?|cftc):/i.test(s)) { var cot = global.JHChartCFTC && global.JHChartCFTC.resolve(s); return cot ? cot.id : ""; }
    if (/^desk:/i.test(s)) return "DESK:" + s.split(":")[1];
    if (/^data:/i.test(s)) return "DATA:" + s.split(":")[1];
    if (/^provider:/i.test(s)) return "provider:" + s.split(":")[1];
    if (/^cq:/i.test(s)) return "CQ:" + s.split(":").slice(1).join(":");
    if (/^cqsnap:/i.test(s)) return "CQSNAP:" + s.split(":").slice(1).join(":");
    if (/^cqarm:/i.test(s)) return "CQARM:" + s.split(":").slice(1).join(":");
    if (/^cqdoc:/i.test(s)) return "CQDOC:" + s.split(":").slice(1).join(":");
    if (/^ciss:/i.test(s)) return "CISS:" + s.split(":").slice(1).join(":");
    if (low === "defillama:total_tvl") return "defillama:tvl:all";
    if (/^defillama:/i.test(s)) return DEFILLAMA_IDS.get(low) || "";
    if (/^regionalfed:/i.test(s)) return REGIONAL_FED_IDS.get(low) || "";
    if (isWarehouse(s)) return s;
    if (/^indicator-bus:/i.test(s)) {
      var id = s.split(":").slice(1).join(":");
      if (/mvrv|sopr|nupl|netflow|ssr/i.test(id)) return "CQ:" + id.replace(/^BTC_?/i, "btc_").toLowerCase();
      return "fred:" + id.split(":").pop();
    }
    var via = lookupSym(s);
    if (via) return via;
    var hit = CURATED.filter(function (x) { return x.s.toLowerCase() === low || x.q.indexOf(low) >= 0; })[0];
    if (hit && hit.s.indexOf(":") >= 0) return hit.s;
    return "";
  }

  function rowExtra(row) {
    var extra = [];
    if (row.first && row.last) extra.push(String(row.first).slice(0, 10) + " → " + String(row.last).slice(0, 10));
    else if (row.first) extra.push("from " + row.first);
    if (row.n) extra.push(Number(row.n).toLocaleString() + (row.kind === "dataset" ? " series" : " obs"));
    if (row.freq) extra.push(row.freq);
    extra.push(row.provider_name || row.provider || row.kind);
    if (row.chartable === false) extra.push("browse");
    return extra.filter(Boolean).join(" · ");
  }

  function mapRow(row) {
    if (!row) return null;
    var id = row.id || row.symbol || row.ticker || "";
    var kind = String(row.kind || row.type || "");
    var name = row.name || row.title || "";
    var provider = String(row.provider || "");
    if (/13f/i.test(id + " " + name) && kind !== "instrument") {
      return { s: "DESK:inst", name: name || "13F book", extra: "SEC 13F · quarterly lagged", type: "desk" };
    }
    if (kind === "series" || (row.chartable && isWarehouse(id))) {
      return { s: id, name: name || id, extra: rowExtra(row), type: "economy" };
    }
    if (kind === "instrument") {
      return { s: row.symbol || id, name: name, extra: [(row.ex || row.exchange || ""), (row.type || "symbol"), (row.provider_name || provider)].filter(Boolean).join(" · "), type: String(row.type || "stock").toLowerCase() };
    }
    if (kind === "dataset" || kind === "indicator_ref") {
      return { s: id, name: name || id, extra: rowExtra(row), type: "dataset" };
    }
    var viaSym = lookupSym(id) || lookupSym(row.symbol);
    if (viaSym) return { s: viaSym, name: name || viaSym, extra: "OpenFIGI / SEC spine", type: "stock" };
    var mapped = chartId(id);
    if (mapped) return { s: mapped, name: name || mapped, extra: rowExtra(row), type: classify(mapped) || kind || "economy" };
    if (id) return { s: id, name: name || id, extra: rowExtra(row), type: kind || "stock" };
    return null;
  }

  function go(s, dest, options) {
    s = String(s || "");
    if (global.JHChartProviderBrowser && (/^(DATA|provider):/i.test(s) || dest === "dataset")) {
      return global.JHChartProviderBrowser.open({id:s,destination:(options && options.destination) || (dest === "dataset" ? "chart" : dest)});
    }
    if (/^CQSNAP:/i.test(s)) {
      var snap = s.replace(/^CQSNAP:/i, "");
      global.location.href = "/crypto/?tab=cq&snap=" + encodeURIComponent(snap);
      return true;
    }
    if (/^CQARM:/i.test(s)) {
      global.location.href = "/crypto/?tab=cq&arm=" + encodeURIComponent(s.replace(/^CQARM:/i, ""));
      return true;
    }
    if (/^CQDOC:/i.test(s)) {
      global.location.href = "/crypto/?tab=cq&doc=" + encodeURIComponent(s.replace(/^CQDOC:/i, ""));
      return true;
    }
    var slug = "";
    if (/^DATA:/i.test(s) || /^provider:/i.test(s)) slug = s.split(":")[1] || "";
    else if (dest === "dataset" && s.indexOf(":") >= 0) slug = /^provider:/i.test(s) ? s.split(":")[1] : s.split(":")[0];
    if (slug && slug !== "search") {
      global.location.href = SERIES_PROV[String(slug).toLowerCase()] ? ("/provider.html?p=" + encodeURIComponent(slug)) : "/data.html";
      return true;
    }
    if (/^DATA:/i.test(s)) {
      global.location.href = "/data.html";
      return true;
    }
    if (!/^DESK:/i.test(s)) return false;
    var tab = s.split(":")[1] || "over";
    if (global.jhOpenDataTypePanel) global.jhOpenDataTypePanel(tab);
    else if (global.jhOpenDataType) global.jhOpenDataType(null);
    return true;
  }

  function dvBars(d, v) {
    var out = [], i;
    if (!d || !v) return out;
    for (i = 0; i < d.length && i < v.length; i++) {
      var t = Math.floor(Date.parse(String(d[i]).length <= 10 ? d[i] + "T00:00:00Z" : d[i]) / 1000);
      var c = +v[i];
      if (!isFinite(t) || t <= 0 || !isFinite(c)) continue;
      out.push({ time: t, open: c, high: c, low: c, close: c, volume: 0 });
    }
    return out;
  }
  function ptsBars(pts) {
    var out = [], i;
    if (!pts) return out;
    for (i = 0; i < pts.length; i++) {
      var p = pts[i];
      var t = Array.isArray(p) ? p[0] : (p.t || p.date);
      var c = Array.isArray(p) ? p[1] : (p.v != null ? p.v : p.value);
      t = Math.floor(Date.parse(String(t).length <= 10 ? t + "T00:00:00Z" : t) / 1000);
      c = +c;
      if (!isFinite(t) || !isFinite(c)) continue;
      out.push({ time: t, open: c, high: c, low: c, close: c, volume: 0 });
    }
    return out;
  }

  function loadJson(url, signal) {
    return fetch(url, { cache: "no-store", signal: signal || undefined }).then(function (r) {
      if (!r.ok) throw new Error("http " + r.status);
      return r.json();
    });
  }

  function loadCQ() {
    if(!global.JHObservationCache)return Promise.reject(new Error("Observation cache unavailable"));
    return global.JHObservationCache.shared().read("series").then(function(result){CQ=result.packet;return result;});
  }
  function loadCISS() {
    if(!global.JHObservationCache)return Promise.reject(new Error("Observation cache unavailable"));
    return global.JHObservationCache.shared().read("ciss").then(function(result){CISS=result.packet;return result;});
  }

  function cqKey(sym) {
    var k = String(sym || "").replace(/^CQ:/i, "");
    return k;
  }

  function mergeDv(a, b) {
    var m = {}, i, t;
    function put(row, prefer) {
      if (!row || !row.d || !row.v) return;
      for (i = 0; i < row.d.length && i < row.v.length; i++) {
        t = String(row.d[i]).slice(0, 10);
        if (!t) continue;
        if (prefer || !m[t]) m[t] = +row.v[i];
      }
    }
    put(a, false);
    put(b, true);
    var dates = Object.keys(m).sort();
    return { d: dates, v: dates.map(function (k) { return m[k]; }) };
  }
  function cqRow(doc, k) {
    k = String(k || "");
    var ser = (doc.series && (doc.series[k] || doc.series[k.toLowerCase()])) || null;
    var twin = (doc.twins && (doc.twins[k] || doc.twins[k.toLowerCase()])) || null;
    if (!ser && doc.series) {
      var want = k.toLowerCase().replace(/^btc_/, "");
      Object.keys(doc.series).forEach(function (id) {
        if (!ser && id.toLowerCase().indexOf(want) >= 0) ser = doc.series[id];
      });
    }
    if (!twin && doc.twins) {
      var want2 = k.toLowerCase().replace(/^btc_/, "");
      Object.keys(doc.twins).forEach(function (id) {
        if (!twin && id.toLowerCase().indexOf(want2) >= 0) twin = doc.twins[id];
      });
    }
    if (ser && twin) return mergeDv(twin, ser);
    return ser || twin || null;
  }

  function loadWarehouse(sym) {
    // Capacity bounds retained complete packets. Eviction aborts and supersedes
    // a pending request; it never publishes its late body into a newer entry.
    var key=String(sym),cache=WAREHOUSE_CACHES.get(key);
    if(cache){WAREHOUSE_CACHES.delete(key);WAREHOUSE_CACHES.set(key,cache);}
    else{
      if(WAREHOUSE_CACHES.size>=WAREHOUSE_CACHE_CAPACITY){var oldest=WAREHOUSE_CACHES.keys().next().value;WAREHOUSE_CACHES.get(oldest).reset();WAREHOUSE_CACHES.delete(oldest);}
      var path=PROXY+"/series?id="+encodeURIComponent(key);
      cache=global.JHObservationCache.create({sources:{warehouse:{paths:[path],valid:function(doc){return doc!==null&&typeof doc==='object'&&!Array.isArray(doc)&&Array.isArray(doc.obs)&&typeof doc.id==='string'&&doc.id.toLowerCase()===key.toLowerCase();}}}});
      WAREHOUSE_CACHES.set(key,cache);
    }
    return cache.read('warehouse');
  }


  function calcShift(iso, years, months) {
    var y = +String(iso).slice(0, 4), m = +String(iso).slice(5, 7);
    var d = String(iso).length >= 10 ? String(iso).slice(8, 10) : "01";
    if (!y || !m) return "";
    y -= years; m -= months;
    while (m < 1) { m += 12; y -= 1; }
    return y + "-" + (m < 10 ? "0" : "") + m + "-" + d;
  }
  function calcMap(obs) {
    var m = Object.create(null), i, p, v;
    if (!Array.isArray(obs)) return m;
    for (i = 0; i < obs.length; i++) {
      p = obs[i];
      if (!Array.isArray(p) || typeof p[0] !== "string") continue;
      v = p[1];
      if (typeof v !== "number" || !isFinite(v)) continue;
      m[p[0]] = v;
    }
    return m;
  }
  async function calcSeries(id) {
    var raw = String(id || ""), body = raw.slice(5), cut = body.indexOf(":"), kind = (cut > 0 ? body.slice(0, cut) : "").toLowerCase(), rest = cut > 0 ? body.slice(cut + 1) : "";
    var obs = [], name = "", unit = "Percent", freq = null;
    if (!global.JHObservationSeries || typeof global.JHObservationSeries.warehouse !== "function" || !global.JHObservationCache) return { d: [], src: "Observation history unavailable: required module not loaded" };
    if (kind === "minus") {
      var parts = rest.split("~");
      if (parts.length !== 2 || !parts[0] || !parts[1]) return { d: [], src: "calc identity rejected" };
      var ea = await loadWarehouse(parts[0]), eb = await loadWarehouse(parts[1]);
      var pa = ea && ea.packet, pb = eb && eb.packet;
      if (!pa || !pb || String(pa.id || "").toLowerCase() !== parts[0].toLowerCase() || String(pb.id || "").toLowerCase() !== parts[1].toLowerCase()) return { d: [], src: "calc inputs unavailable" };
      if (!/Exports of goods and services/i.test(pa.name || "") || !/Imports of goods and services/i.test(pb.name || "")) return { d: [], src: "calc inputs are not exports and imports" };
      var mb = calcMap(pb.obs), ma = calcMap(pa.obs);
      Object.keys(ma).sort().forEach(function (dt) {
        if (Object.prototype.hasOwnProperty.call(mb, dt)) obs.push([dt, ma[dt] - mb[dt]]);
      });
      name = "Exports minus imports of goods and services";
      unit = "Current US$";
      freq = pa.freq || null;
    } else if (kind === "yoy" || kind === "mom") {
      var got = await loadWarehouse(rest), pkt = got && got.packet;
      if (!pkt || String(pkt.id || "").toLowerCase() !== rest.toLowerCase() || !Array.isArray(pkt.obs)) return { d: [], src: "calc input unavailable" };
      if (!/index/i.test(pkt.name || "")) return { d: [], src: "calc input is not an index" };
      var base = calcMap(pkt.obs);
      Object.keys(base).sort().forEach(function (dt) {
        var prev = kind === "mom" ? calcShift(dt, 0, 1) : calcShift(dt, 1, 0);
        var b = base[prev];
        if (b === undefined || b === 0) return;
        obs.push([dt, 100 * (base[dt] / b - 1)]);
      });
      name = (kind === "mom" ? "Month-over-month percent of " : "Year-over-year percent of ") + (pkt.name || rest);
      unit = "Percent";
      freq = pkt.freq || null;
    } else if (kind === "arith") {
      var tokens = String(rest || "").toUpperCase().split("~");
      if (tokens.length < 2 || tokens.length > 24) return { d: [], src: "calc identity rejected" };
      var leaves = [], ti, tok, depth = 0, anyScale = false;
      for (ti = 0; ti < tokens.length; ti++) {
        tok = tokens[ti];
        if (/^N:[0-9]+(?:\.[0-9]+)?$/.test(tok)) { depth++; continue; }
        if (/^S:(?:FRED:[A-Z0-9]+|WORLDBANK:[A-Z0-9.]+:[A-Z0-9]{2,3})$/.test(tok)) {
          var sid = tok.slice(2);
          if (leaves.indexOf(sid) < 0) leaves.push(sid);
          depth++;
          continue;
        }
        if (tok === "NEG") { if (depth < 1) return { d: [], src: "calc identity rejected" }; continue; }
        if (tok !== "ADD" && tok !== "SUB" && tok !== "MUL" && tok !== "DIV") return { d: [], src: "calc identity rejected" };
        if (depth < 2) return { d: [], src: "calc identity rejected" };
        depth--;
        if (tok === "MUL" || tok === "DIV") anyScale = true;
      }
      if (depth !== 1 || !leaves.length) return { d: [], src: "calc identity rejected" };
      var maps = Object.create(null), pkt0 = null;
      for (ti = 0; ti < leaves.length; ti++) {
        var gotA = await loadWarehouse(leaves[ti]), pktA = gotA && gotA.packet;
        if (!pktA || String(pktA.id || "").toUpperCase() !== leaves[ti] || !Array.isArray(pktA.obs)) return { d: [], src: "calc inputs unavailable" };
        maps[leaves[ti]] = calcMap(pktA.obs);
        if (!pkt0) pkt0 = pktA;
      }
      var dates = Object.keys(maps[leaves[0]]);
      for (ti = 1; ti < leaves.length; ti++) {
        var keep = maps[leaves[ti]];
        dates = dates.filter(function (dt) { return Object.prototype.hasOwnProperty.call(keep, dt); });
      }
      dates.sort();
      for (ti = 0; ti < dates.length; ti++) {
        var dt = dates[ti], st = [], ok = true, vi;
        for (vi = 0; vi < tokens.length && ok; vi++) {
          tok = tokens[vi];
          if (tok.slice(0, 2) === "N:") st.push(+tok.slice(2));
          else if (tok.slice(0, 2) === "S:") st.push(maps[tok.slice(2)][dt]);
          else if (tok === "NEG") st.push(-st.pop());
          else {
            var rb = st.pop(), ra = st.pop(), rv = NaN;
            if (tok === "ADD") rv = ra + rb;
            else if (tok === "SUB") rv = ra - rb;
            else if (tok === "MUL") rv = ra * rb;
            else if (rb !== 0) rv = ra / rb;
            if (typeof rv !== "number" || !isFinite(rv)) ok = false;
            else st.push(rv);
          }
        }
        if (ok && st.length === 1) obs.push([dt, st[0]]);
      }
      name = "Arithmetic of stored series";
      unit = anyScale ? "Ratio" : ((pkt0 && pkt0.unit) || "Calculated");
      freq = (pkt0 && pkt0.freq) || null;
    } else return { d: [], src: "calc identity rejected" };
    if (obs.length < 8) return { d: [], src: "calc produced fewer than 8 observations" };
    var doc = { id: raw, provider: "calc", provider_name: "Calculated", name: name, unit: unit, freq: freq, source: "calc", obs: obs };
    var parsed = global.JHObservationSeries.warehouse(doc, raw, PROXY + "/series?id=" + encodeURIComponent(raw));
    parsed.src += kind === "arith" ? " · calculated from stored observations of the named inputs; not a published series" : " · calculated from stored observations of the same instrument";
    return parsed;
  }
  async function klines(sym) {
    var s = String(sym || "");
    if (/^defillama:/i.test(s)) { s=chartId(s); if(!s)return {d:[],src:"DefiLlama history unavailable: choose an exact reviewed TVL definition"}; }
    if (/^regionalfed:/i.test(s)) { s=chartId(s); if(!s)return {d:[],src:"Regional Fed history unavailable: select an exact reviewed series and adjustment"}; }
    if (/^calc:/i.test(s)) return calcSeries(s);
    if (isWarehouse(s) && !/^CQ:|^CISS:|^DESK:|^DATA:/i.test(s)) {
      if(!global.JHObservationSeries||typeof global.JHObservationSeries.warehouse!=="function"||!global.JHObservationCache)return {d:[],src:"Observation history unavailable: required module not loaded"};
      var received=await loadWarehouse(s), parsed=global.JHObservationSeries.warehouse(received.packet,s,PROXY+"/series?id="+encodeURIComponent(s));
      parsed.evidence.transport_cache=received.cache;
      if (/^regionalfed:/i.test(String(sym)) && String(sym)!==s) { parsed.evidence.requested_id=String(sym); parsed.evidence.chart_alias={requested:String(sym),resolved:s,rule:"Exact reviewed regional Fed identifier; source equivalence unverified"}; }
      if (/^defillama:/i.test(String(sym)) && String(sym)!==s) { parsed.evidence.requested_id=String(sym); parsed.evidence.chart_alias={requested:String(sym),resolved:s,rule:"Exact reviewed DefiLlama identifier or explicit default-TVL alias; source equivalence unverified"}; }
      parsed.src+=" · download "+received.cache.state+" · upstream original evidence unverified";
      return parsed;
    }
    if (/^CQSNAP:|^CQARM:|^CQDOC:/i.test(s)) return null;
    if (/^CQ:|^CISS:/i.test(s)) {
      var history = global.JHObservationSeries;
      if (!history || !global.JHObservationCache) return { d: [], src: "Observation history unavailable: required module not loaded" };
      if (/^CQ:/i.test(s)) {
        if (global.JHCqFuse && typeof global.JHCqFuse.klines === "function") {
          try {
            var fused = await global.JHCqFuse.klines(s);
            if (fused && fused.evidence && fused.evidence.contract === history.contract) return fused;
          } catch (eFuse) {}
        }
        var cq = await loadCQ(), primary = history.cq(cq.packet, s);
        primary.evidence.transport_cache = cq.cache; primary.src += " · download " + cq.cache.state + " · source freshness unverified"; return primary;
      }
      var ciss = await loadCISS(), stress = history.ciss(ciss.packet, s);
      stress.evidence.transport_cache = ciss.cache; stress.src += " · download " + ciss.cache.state + " · source freshness unverified"; return stress;
    }
    return null;
  }

  global.JHChartCatalog = {
    chips: chips,
    tabMatch: tabMatch,
    aliasHits: aliasHits,
    search: search,
    suggest: suggest,
    ensureIndex: ensureIndex,
    indexStatus: indexStatus,
    lookupSym: lookupSym,
    keepId: keepId,
    isWarehouse: isWarehouse,
    classify: classify,
    chartId: chartId,
    mapRow: mapRow,
    go: go,
    klines: klines,
    curated: CURATED,
    cqOscSpecs: cqOscSpecs,
    cqMeta: CQ_META
  };
})(typeof window !== "undefined" ? window : globalThis);
