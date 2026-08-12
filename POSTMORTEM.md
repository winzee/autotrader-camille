# Post-mortem — session du 2026-08-05 au 2026-08-11

Compte rendu des pannes rencontrées, des correctifs appliqués, et **des erreurs de
raisonnement qui les ont causées ou prolongées**. Écrit comme point de départ d'un
code review : chaque entrée nomme ce qui a été supposé sans preuve.

---

## Partie 1 — Ce qu'on a appris du fonctionnement de Facebook

Ces constats sont mesurés, pas déduits. Ils conditionnent tout le reste.

### Le rendu et les données sont deux choses distinctes

- Depuis le 2026-08-06, les cartes de recherche **n'affichent plus** le kilométrage
  ni la catégorie. `innerText` ne voit que : badge / prix / titre / lieu.
- **Les données sont toujours là**, dans le JSON qui construit ces cartes :
  `custom_sub_titles_with_rendering_flags:[{"subtitle":"149K km"}]` et
  `marketplace_listing_category_id`.
- Vérifié sur trois formes d'URL (`/search`, `/vehicles`, `search+category_id`) :
  0/24, 0/20, 0/24 cartes portent le kilométrage au rendu. Aucune URL ne le ramène.
- **Leçon** : quand un champ « disparaît », vérifier `page_source` et les réponses
  GraphQL avant de conclure. `innerText` ne montre que le visible.

### Le fil est virtualisé et non déterministe

- Les cartes hors écran sont **démontées** du DOM. Après 6 scrolls, le DOM contenait
  56 cartes *entièrement différentes* des 24 initiales.
- Le blob SSR de `page_source` n'est **jamais mis à jour** après le chargement
  initial : il est resté à 23 paires pendant que le DOM en cyclait des centaines.
- Les lots suivants arrivent par **XHR GraphQL**. Le hook `fetch`/`XMLHttpRequest`
  doit être installé via `Page.addScriptToEvaluateOnNewDocument` : injecté après la
  navigation, il capte **0** réponse (le bundle FB a déjà sa propre référence à
  `window.fetch`). Injecté avant : 17 réponses.
- **Le fil ne resert presque jamais la même chose.** Sur 264 cartes vues avec 700
  annonces connues en base : 8 recoupements. Sur 360 cartes : 16. Deux sessions
  successives voient des tranches largement disjointes.
- Le tri `creation_time_descend` est respecté à ~97 %, mais le flux **redémarre** :
  il descend jusqu'à ~9 h d'ancienneté vers la position 90, puis repart à 0,7 h en
  position 120. Deux séquences décroissantes entrelacées.

### La profondeur du fil

- Un test a atteint **1685 annonces sur 59 scrolls** avec `has_next_page` encore
  vrai et le curseur à la page 70. Le fil ne se tarit pas facilement.
- Un run complet, fenêtre visible, a vu **1633 cartes**. Les deux chiffres se
  ressemblent : c'est probablement l'ordre de grandeur du fond du flux sur 7 jours.
- Cadence : ~24 nouvelles cartes par scroll au début, jusqu'à 48 plus loin.

### La pagination : `has_next_page`

- Chaque réponse porte un `page_info` avec `has_next_page` et `end_cursor`.
- **Une page contient de nombreuses connexions GraphQL** (commentaires, suggestions,
  notifications), chacune avec son propre `has_next_page`. Le HTML SSR en contenait
  2, dont aucune n'appartenait au fil Marketplace.
- **Discriminant** : seul le curseur du fil Marketplace contient un champ `pg`
  (`"end_cursor":"{\"pg\":57,...`).

### Le contenu servi

- La catégorie « Véhicules » (`807311116002614`) **inclut les motos, scooters et
  VTT**. Aucun champ ne les distingue : `vehicle_specifications` est entièrement
  nul pour un Honda Ruckus comme pour une auto, et il n'existe pas de
  `vehicle_type`.
- Le fil est **rempli de non-véhicules** : 32 % sur un balayage (moteurs,
  transmissions, chaloupes, cartes Pokémon, gazebo, lift de garage). La corrélation
  est parfaite : les 146 cartes en catégorie Véhicules avaient toutes un
  kilométrage, les 22 autres aucune.
- Certains sous-titres sont **en milles**, pas en kilomètres (`156K miles`).
- Des annonces sont republiées sous des `ad_id` distincts (une même BMW vue 3 fois,
  une Nissan Leaf 5 fois dans des villes différentes).

### Les pages de détail

- Une annonce vendue ou expirée **redirige** vers
  `.../marketplace/<ville>/?unavailable_product=1` et affiche
  « This listing isn't available anymore ».
- **Piège** : les champs `is_sold` / `is_live` de cette page décrivent les
  suggestions latérales « Today's picks », **pas l'annonce consultée**. Ils
  affichaient `is_sold:false` / `is_live:true` sur une annonce définitivement morte.
- La redirection prend un délai variable. Tester l'*absence* de marqueurs de mort
  produit des faux négatifs.
- Coût réel mesuré : **13,3 s par annonce** de bout en bout (676 annonces en
  150 minutes), dont 5-7 s de pause délibérée.

### La session et l'authentification

- Un Chrome lancé **par chromedriver** est re-challengé en 2FA à chaque run, et
  Facebook **supprime `c_user`/`xs` côté serveur**. Un Chrome lancé normalement,
  auquel Selenium s'attache via `--remote-debugging-port`, garde sa session
  (vérifié sur redémarrage à froid).
- Cookies pertinents : `c_user` + `xs` (session, expiration 1 an), `dbln` (device
  based login, évite de redemander le 2FA), `checkpoint` (challenge de sécurité en
  cours).
- Un `--user-agent` forcé est inutile : Chrome 130 émet déjà exactement cette
  chaîne, et la falsifier la désynchronise des client hints `Sec-CH-UA`.

### **Le comportement le plus coûteux : Chrome met en veille les fenêtres cachées**

- Une fenêtre en arrière-plan ou masquée subit : timers ramenés à ~1 tick/minute,
  `requestAnimationFrame` arrêté, `IntersectionObserver` qui ne se déclenche plus.
- Le défilement infini de Marketplace **dépend précisément de ces mécanismes** pour
  demander le lot suivant. `scrollTo()` s'exécute, mais plus personne n'écoute.
- **Un fil bridé est indiscernable d'un fil épuisé.**
- Mesuré : un run bloqué à 2 annonces retenues pendant 92 s est reparti **à
  l'instant où la fenêtre est passée en avant-plan**, atteignant 50 en une minute,
  puis 1633 cartes.
- Correctif : `--disable-background-timer-throttling`,
  `--disable-backgrounding-occluded-windows`, `--disable-renderer-backgrounding`.
- **Ce comportement, purement navigateur, explique tous les « flux épuisé » à 24 ou
  264 cartes** qui nous ont fait chasser pendant deux jours des théories de
  détection de bot et de bridage serveur, toutes fausses.

---

## Partie 2 — Journal des pannes

Format : symptôme → cause → correctif → **ce que j'ai mal fait**.

### P1. Login Facebook en boucle, 2FA à chaque run

**Symptôme** : login demandé à chaque exécution ; la fenêtre se ferme et la session
est reperdue.

**Causes (deux, superposées)** :
1. chromedriver lançait Chrome avec `--enable-automation` et
   `navigator.webdriver = true`, sur un binaire re-signé ad hoc. Facebook y voyait
   un appareil non fiable, re-challengeait en 2FA, puis **effaçait les cookies de
   session côté serveur**.
2. `_maybe_login_wait()` détectait le login via `"/login" in url` ou
   `input[name=email]`. La page 2FA n'a **ni l'un ni l'autre** → le script concluait
   « connecté », rechargeait la page de recherche **par-dessus le 2FA en cours**,
   puis quittait 17 s plus tard. D'où « la page se referme ».

**Correctifs** : mode attach (Chrome normal + `--remote-debugging-port`, Selenium via
`debuggerAddress`) ; détection par le cookie `c_user` ; timeout 300 → 600 s ; retrait
du spoof d'UA.

**Mes erreurs** : aucune majeure ici — j'ai vérifié la base de cookies et
l'historique avant d'agir. C'est le seul diagnostic de la session où je n'ai rien
supposé.

---

### P2. 421 cartes rejetées, 0 retenue

**Symptôme** : `Saw 421 non-matching cards, 0 passing`.

**Cause** : Facebook a cessé de rendre le kilométrage dans les cartes.
`reject_reason()` s'en servait comme garde-fou « est-ce un véhicule » → rejet à 100 %.

**Mon erreur — la plus grave en termes de conclusion** : j'ai vérifié `innerText` et
trois variantes d'URL, constaté 0/24, et **conclu que la donnée était irrécupérable**.
J'ai écrit dans CLAUDE.md « no URL recovers it » comme un fait établi, et déplacé le
garde-fou au niveau détail — ce qui coûtait ~28 min de fetches par run.

C'est l'utilisateur qui a demandé de chercher dans le HTML. La donnée était là,
intacte, dans le JSON. **Je n'avais jamais regardé `page_source`.** J'ai documenté
une conclusion fausse avec assurance après une recherche incomplète.

**Correctif final** : `harvest_card_json()` reconstruit `id → {kilométrage,
catégorie}` depuis le blob SSR et les réponses GraphQL captées. Résultat : 32 % du
flux rejeté gratuitement, 0 rejet pour kilométrage manquant.

---

### P3. Arrêt prématuré du scroll (1/2) — `scrollHeight`

**Symptôme** : 21 annonces collectées, `KNOWN_SKIP: 0` malgré 331 annonces connues.

**Cause** : arrêt après 2 mesures identiques de `document.body.scrollHeight`, soit
~8 s de patience. Le flux contenait 456 annonces.

**Correctif** : arrêt sur « aucune carte inédite » au lieu de la hauteur.

**Mon erreur** : j'ai remplacé un signal fondé sur un compteur trop court par **un
autre signal fondé sur un compteur trop court** (4 tours ≈ 16 s). Je n'ai pas mesuré
la latence réelle d'un lot avant de choisir le seuil. Il a fallu deux itérations
supplémentaires pour arriver à une patience **temporelle** avec recul progressif.

---

### P4. `seen_ad_ids` toujours vide

**Symptôme** : `seen_ad_ids=0` dans tous les logs ; chaque run refaisait les mêmes
annonces.

**Causes (deux)** :
1. `ad_id` lu en `float64` → `astype(str)` donnait `'1588888658875172.0'`, jamais
   égal aux ids des cartes.
2. `existing_df["model"] == model_canonical` avec `model_canonical = None` →
   `== None` vaut False partout en pandas.

**Correctifs** : `_ad_id_str()`, `dtype={"ad_id": str}`, portée par source seule
quand `model_canonical` est nul.

**Mon erreur** : **je n'ai corrigé que le côté lecture.** Le chemin AutoTrader et
`collapse_cross_source_duplicates()` relisaient le CSV sans `dtype` et le
réécrivaient en float — la corruption revenait à chaque run. J'avais réparé les
données la veille ; le lendemain, 666 des 672 ids étaient re-corrompus. Il a fallu
le code review pour que je protège les **cinq** `read_csv` du projet.

---

### P5. `_ad_id_str` corrompait ce qu'elle protégeait

**Symptôme** : deux annonces revenaient obstinément « indéterminé » dans le balayage,
alors qu'elles résolvaient en 4 s en test isolé.

**Cause** : la fonction repassait par `float` **même quand la chaîne était déjà
exacte**. Au-delà de 2⁵³ :

```
37359594907019423  ->  37359594907019424
27949647774674278  ->  27949647774674280
```

Le balayage ouvrait donc une annonce inexistante. 24 lignes touchées, qui échappaient
aussi à `seen_ad_ids` depuis le début.

**Correctif** : retourner telle quelle une chaîne entièrement numérique.

**Mon erreur** : la fonction écrite spécifiquement pour réparer la corruption par
float **contenait elle-même la corruption par float**. Je n'avais testé que le cas
`'123.0'`, pas le cas déjà propre.

---

### P6. Motos indistinguables des autos

**Cause** : Facebook ne fournit aucun discriminant (voir Partie 1).

**Correctif** : `_FB_NON_CAR_TITLE_RX`, à frontières de mots, validé contre les 643
titres du CSV.

**Mes erreurs (deux)** :
1. J'ai failli utiliser des mots-clés naïfs. Le test a montré que « sport »
   détruisait 8 Kia Sportage, « cross » 6 Subaru Crosstrek, « quad » un Dodge Ram
   Quad Cab. **Sans ce test, j'aurais supprimé de vraies annonces.**
2. Ma première version, `cbr\d+`, exigeait des chiffres → « (20+) 2011 Honda CBR »
   est passée. Et j'avais écrit dans le commentaire « validated against all 643 CSV
   titles: it rejects exactly the 3 known powersport rows » — une **affirmation qui
   a cessé d'être vraie** et que le code review a relevée comme non reproductible.

---

### P7. Doublons internes survivants

**Cause** : la clé de `collapse_cross_source_duplicates()` exigeait `make` **et**
`model` non nuls — or ce sont exactement les lignes les plus susceptibles d'être des
reposts (« 2009 Pontiac Vibe » ne donne aucune marque pour un profil japonais/coréen).
Une BMW figurait 3 fois.

**Correctif** : clé sur année + kilométrage + prix, `make`/`model` tolérant les nuls.

---

### P8. Plafond de rejets cumulés tronquant les runs

**Symptôme** : `Saw 402 non-matching cards (cap=400) — stopping with 348 passing`
sur une demande de 1000.

**Cause** : `max_nonmatching` comptait les rejets **cumulés**. C'est une limite de
travail total déguisée en détecteur de « flux dégradé » : à taux de bruit stable,
elle finit toujours par sauter. Mon correctif du garde-fou catégorie (P2) l'a rendue
atteignable en rendant les rejets abondants.

**Correctif** : rejets **consécutifs**, remis à zéro par toute annonce gardée.

**Mes erreurs** :
1. **Calibration sur un échantillon trop court.** J'ai mesuré une séquence maximale
   de 21 sur 750 cartes et fixé 60. Un run plus profond a montré 53 — 7 de marge.
   Il a fallu monter à 150. Les séquences de bruit s'allongent avec la profondeur ;
   calibrer sur une run courte donne un chiffre trompeur.
2. **Analyse contaminée.** J'ai d'abord annoncé « 1249 cartes, 598 gardées » en
   filtrant `fb_card_trace.log` par heure du jour, ce qui **mélangeait deux runs**
   (le fichier n'a pas de dates). Il a fallu segmenter par recul horaire.

---

### P9. Conditions d'arrêt incohérentes

**Constat** : `--limit` ne s'appliquait **pas du tout** à AutoTrader
(`MAX_LISTINGS = None`, constante morte), et un plafond codé en dur de 40 scrolls
**écrasait silencieusement** `--limit 1000` en le ramenant à ~435.

**Correctif** : `limits.<source>.max_new_listings` dans le YAML, `--limit` surchargeant
les deux sources, garde-fous internalisés, `_scroll_budget()` **dérivé de la cible**.

**Mon erreur** : j'ai écrit à l'utilisateur que les garde-fous seraient
« dimensionnés pour ne jamais couper avant la limite demandée » — **et le détecteur
de 4 scrolls a fait exactement ça** le soir même. J'avais calibré sérieusement le
garde-fou des rejets (mesures, distribution) et **deviné** l'autre. Une promesse
tenue sur un mécanisme et pas sur l'autre.

---

### P10. Boucle infinie dans la pagination AutoTrader

**Cause** : le repli ne sortait que si une page n'avait **ni** lien nouveau **ni**
lien du tout. Une page re-servie (paramètre `page` ignoré, ou filtre postal vidant
tout) bouclait indéfiniment. Atteignable seulement si `numberOfPages` est absent.

**Correctif** : compteur de pages consécutives sans nouveauté + plafond absolu.

**Mon erreur** : j'ai rendu le compteur **inconditionnel**, alors que mon propre
commentaire disait « fallback for the no-metadata case only ». Trois pages vidées par
le filtre postal auraient tronqué une recherche légitime. Relevé par le code review.

---

### P11. Balayage des annonces vendues

**Besoin** : le fil ne peut pas retirer une annonce vendue (elle cesse simplement
d'apparaître). `delete_sold_listings()` rouvre chaque annonce.

**Mes erreurs (quatre)** :
1. **Test par absence.** Première version : attente fixe de 2,5 s puis test de
   l'*absence* de marqueurs de mort. La redirection n'étant pas terminée, une
   annonce vendue a été lue comme vivante — faux négatif silencieux, qui frappe
   surtout la première annonce d'un balayage quand le navigateur est froid.
   Correctif : exiger une **preuve positive** d'un côté ou de l'autre, et renvoyer
   « indéterminé » sinon.
2. **Fenêtre de sondage trop courte** (14 s) sous le rythme réel du balayage.
3. **Estimation de durée fausse** : annoncée ~90 min, réelle 150 min. Ma formule
   supposait 8 s par annonce, la mesure donne 13,3 s.
4. **Hypothèse présentée comme un fait** : j'ai affirmé que « 676 navigations en
   90 minutes » avaient fait brider la session. C'était faux sur les deux nombres
   (150 minutes, soit une navigation toutes les 13 s — un rythme lent), et
   l'hypothèse elle-même n'a jamais été vérifiée. Je l'avais **écrite dans un
   commentaire de code**. Requalifiée depuis.

---

### P12. Lecture du drapeau d'un inconnu — `has_next_page`

**Symptôme** : un run `--limit 10000` s'est terminé après **17 annonces**.

**Cause** : je prenais la **dernière** occurrence de `has_next_page` dans la réponse.
Le HTML SSR en contenait 2, la dernière valant `false`, et **aucune n'appartenait au
fil Marketplace**.

**Correctif** : `_feed_has_next_page()`, qui ne lit que les blocs `page_info` dont le
curseur porte un champ `pg`.

**Mon erreur — la plus embarrassante** : j'ai construit une **condition d'arrêt** sur
un champ non qualifié. Et c'est exactement le piège que j'avais **déjà documenté**
quelques heures plus tôt à propos de `is_sold` / `is_live` sur les pages de détail,
qui décrivent les suggestions latérales. J'ai répété ma propre leçon écrite.

---

### P13. Marquage massif et faux de 564 annonces comme vendues

**Symptôme** : `Marking 564 FB listings as deleted`. Échantillon de 16 rouvertes :
**15 encore en vente**.

**Cause** : une recherche Marketplace est bornée par `daysSinceListed`. Une annonce
publiée il y a 8 jours **ne peut pas** apparaître dans une recherche sur 7 jours,
même parfaitement active. Le code lisait « absente de ce scrape » comme « disparue ».

**Correctif** : **suppression complète de l'inférence de disparition** du chemin
Facebook. Elle ne peut pas être rendue correcte sans stocker la date de publication
de chaque annonce. `delete_sold_listings()` fait ce travail correctement.

**Mes erreurs — c'est le cas que l'utilisateur a nommé, et il a raison** :

1. **J'ai laissé en place une inférence dont je savais qu'elle était invalide.**
   J'avais mesuré et *documenté* que le fil est non déterministe : 8 recoupements sur
   264 cartes, 16 sur 360. J'ai écrit noir sur blanc que « Facebook ne resert quasiment
   jamais ce qu'il a déjà montré » — puis j'ai laissé tourner un code qui déduit la
   disparition de la non-observation. Les deux affirmations sont incompatibles et
   j'ai tenu les deux en même temps.

2. **J'ai passé la session à corriger les symptômes de ce bloc sans questionner sa
   prémisse.** J'ai corrigé `scraped_ad_ids` pour qu'il inclue les annonces connues
   revues (P4/B1), corrigé le masque `fb_mask` pour les requêtes génériques, corrigé
   l'appariement des `ad_id` — trois correctifs *à l'intérieur* d'une logique qui
   n'aurait pas dû exister. Je n'ai jamais reculé d'un pas pour demander si la
   question posée était la bonne.

3. **J'ai ajouté un garde-fou qui mesure la mauvaise chose.** Le contrôle de santé
   vérifie que le scrape était **gros** (≥50 % des lignes actives retrouvées), pas
   que le flux était **en mesure de témoigner** sur ces lignes. Avec 1633 cartes vues,
   le seuil passait sans difficulté. Un garde-fou mal ciblé donne une fausse
   assurance : il m'a permis de croire le problème traité.

4. **Ironie du calendrier** : le garde-fou avait bloqué le marquage à plusieurs
   reprises pendant la session (« scrape looks partial »), ce que j'ai présenté
   comme une preuve que le mécanisme fonctionnait. Il masquait en réalité un bug de
   fond, et la seule fois où il a laissé passer, il a détruit 564 lignes.

---

### P14. Kilométrage en milles interprété comme kilomètres

**Cause** : le regex acceptait `km` **n'importe où** après le nombre.
`"156K miles"` → `"156kmiles"` → le `km` de `kmiles` matchait → **156 km**.

**Dégâts** : un RAV4 2008 à 172 km, une Mazda3 2011 à 163 km, ~17 lignes suspectes.

**Correctif** : unité ancrée en fin de chaîne, conversion milles → km.

**Mon erreur** : bug préexistant, mais je l'ai repéré dans les logs d'un run **et j'ai
corrigé le code pendant que le run tournait**, sachant que Python avait déjà chargé le
module. Le correctif ne s'appliquait donc pas au run en cours — je l'ai dit, mais
c'était une correction à moitié utile au moment où je l'ai faite.

---

## Partie 3 — Motifs récurrents dans mes erreurs

Pour le code review : ces schémas se sont répétés, pas seulement les bugs.

1. **Conclure « c'est impossible » après une recherche incomplète, et le documenter
   comme un fait.** P2 (kilométrage « irrécupérable »), P11 (bridage « certain »).
   À chaque fois j'avais vérifié un chemin et pas les autres.

2. **Corriger la lecture, pas l'écriture.** P4 : la corruption venait du côté écriture
   et revenait à chaque run.

3. **Choisir des seuils par intuition plutôt que par mesure** — puis les défendre.
   P3 (4 scrolls), P8 (60 rejets). Quand j'ai mesuré, les vrais chiffres étaient 2 à 3
   fois plus grands.

4. **Compter des tours au lieu de mesurer du temps.** P3, P8. Quatre coups d'œil en
   16 secondes ne valent pas quatre fois plus d'information qu'un seul.

5. **Faire confiance à un champ JSON sans vérifier à quel objet il appartient.**
   P12 (`has_next_page`), et la même chose sur `is_sold`/`is_live` — que j'avais
   pourtant déjà documentée.

6. **Corriger les symptômes d'un bloc sans questionner sa prémisse.** P13. Trois
   correctifs successifs à l'intérieur d'une logique qui devait disparaître.

7. **Construire un garde-fou qui mesure autre chose que le risque réel.** P13. Pire
   qu'aucun garde-fou : il rassure.

8. **Écrire des affirmations de validation dans les commentaires qui cessent d'être
   vraies.** P6 (« rejects exactly the 3 known rows »).

9. **Chasser une explication sophistiquée avant d'avoir éliminé la simple.** Deux
   jours sur la détection de bot et le bridage serveur, alors que la cause était le
   comportement documenté d'un navigateur sur les fenêtres cachées. C'est
   l'utilisateur qui l'a trouvée.

---

## Partie 4 — Points ouverts

- **Lignes en milles à réparer** dans `emile_suv_listings.csv` : RAV4 2008 à 172 km,
  Mazda3 2011 à 163 km, ~15 autres suspectes. Le sous-titre d'origine est
  reconstituable en rouvrant l'annonce.
- **`has_next_page` n'a jamais été capté** pendant le dernier run complet
  (`None` d'un bout à l'autre), alors qu'un test du matin le trouvait avec ses
  curseurs `pg`. Le signal principal ne se déclenche pas — à investiguer.
- **`--keep-browser` empêche les nouveaux drapeaux anti-bridage de s'appliquer** : le
  run suivant se rattache au Chrome existant, lancé sans eux. Fermer entre deux runs.
- **Le marquage des disparitions n'existe plus côté Facebook.** Seul
  `delete_sold_listings()` retire des annonces. Il coûte ~13,3 s par annonce, soit
  ~3 h pour 1362 lignes, et tourne par défaut avant chaque scrape.
- **Pertinence des résultats** : sur 35 lignes ajoutées un jour donné, 4 seulement
  mentionnaient AWD. Le `regex: ".*"` d'Émile est délibéré, mais la requête `awd` de
  Facebook est très permissive. L'utilisateur a explicitement choisi de trier
  lui-même — noté ici pour mémoire, pas comme un défaut à corriger.
- **Code mort** relevé par le code review et non traité : `extract_apollo_listings()`,
  `_APOLLO_LISTING_RX`, `_decode_jsonish`, `is_target_vehicle()`, le paramètre
  `page_num` de `_filter_with_listings()`.
- **`scroll_to_load_all()`** (`bmw_x3_scraper.py`) reste un `while True` sans plafond
  d'itérations — seule boucle non bornée restante, préexistante.

---

## Résolution des points ouverts (2026-08-11, code review + correctifs)

- **`has_next_page` jamais capté** — cause trouvée : `_JSON_PAGEINFO_RX` capturait
  le span entre `page_info` et `has_next_page`, donc si FB sérialise le drapeau
  AVANT `end_cursor`, le discriminant `pg` tombait hors du span et le verdict était
  `None` en permanence. Réécrit ordre-indépendant (`_feed_page_info()`, fenêtre
  bornée, testé dans les deux ordres) ; chaque verdict capté est maintenant loggé.
- **`--keep-browser`** — le scraper avertit désormais quand il s'attache à un
  Chrome qu'il n'a pas lancé (drapeaux anti-bridage possiblement absents).
- **Marquage des disparitions côté FB** — revue confirmée : quatre écritures
  d'`is_deleted` dans le projet, aucune atteignable pour une ligne FB depuis un
  scrape de recherche. Seul `delete_sold_listings()` retire des lignes FB.
- **Code mort** — tout supprimé (Apollo, `is_target_vehicle`,
  `_row_matches_scrape_params` et orphelins, `page_num`, faux « Tier 3 »).
- **`scroll_to_load_all()`** — borné à 30 itérations, log quand le plafond mord.
- **Lignes en milles** — volontairement non réparées : décision utilisateur du
  2026-08-11 (« on ne court pas après les petites erreurs »). Le parseur corrigé
  empêche toute nouvelle occurrence ; ~15 lignes anciennes restent telles quelles.
- **Non traité, assumé** : la requête FB `awd` très permissive (tri manuel choisi
  par l'utilisateur).
