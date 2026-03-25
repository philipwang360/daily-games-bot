#!/usr/bin/env python3
"""
Zaily Games Leaderboard Bot for Discord — Per-Game Edition
"""

import re, os, sqlite3, logging
from datetime import datetime, timedelta, timezone, time as dt_time
from collections import defaultdict
from typing import Optional
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

TOKEN      = os.getenv("DAILY_GAMES_BOT_TOKEN")
DB_PATH    = os.getenv("DB_PATH", "zailygames.db")
PREFIX     = os.getenv("BOT_PREFIX", "!zgb ") #Updated

log = logging.getLogger("zailygames")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

MEDALS = ["🥇", "🥈", "🥉"]


# ═══════════════════════════════════════════════════════════════════
#  PARSER REGISTRY
# ═══════════════════════════════════════════════════════════════════

_PARSERS: list[dict] = []
_GAME_META: dict[str, dict] = {}


class GameResult:
    __slots__ = ("game", "score", "max_score", "display")
    def __init__(self, game, score, max_score, display):
        self.game      = game
        self.score     = score
        self.max_score = max_score
        self.display   = display


def game_parser(name: str, pattern: str, *,
                lower_is_better: bool = False, icon: str = "🎮"):
    compiled = re.compile(pattern, re.DOTALL | re.IGNORECASE)
    def deco(fn):
        entry = dict(name=name, pat=compiled, fn=fn, low=lower_is_better, icon=icon)
        _PARSERS.append(entry)
        _GAME_META[name.lower()] = entry
        return fn
    return deco


def parse_message(text: str) -> list[GameResult]:
    out: list[GameResult] = []
    for p in _PARSERS:
        m = p["pat"].search(text)
        if not m:
            continue
        ex = p["fn"](m)
        if ex is None:
            continue
        score, mx, disp = ex
        out.append(GameResult(p["name"], score, mx, disp))
    return out


def _match_game(query: str) -> Optional[str]:
    q = query.lower().strip()
    for p in _PARSERS:
        if p["name"].lower() == q:
            return p["name"]
    hits = [p["name"] for p in _PARSERS if q in p["name"].lower()]
    if len(hits) == 1:
        return hits[0]
    hits = [p["name"] for p in _PARSERS if p["name"].lower().startswith(q)]
    if len(hits) == 1:
        return hits[0]
    return query


# ═══════════════════════════════════════════════════════════════════
#  GAME PARSERS — ADD NEW GAMES HERE
# ═══════════════════════════════════════════════════════════════════

@game_parser("Wordle", r"Wordle\s+[\d,]+\s+([X\d])/6",
             lower_is_better=True, icon="🟩")
def _wordle(m):
    v = m.group(1).upper()
    if v == "X":
        return 7, 6, "X/6"
    return int(v), 6, f"{v}/6"


@game_parser("Pokédle", r"#Pokédle[\s\S]*?in\s+(\d+)\s+shots?",
             lower_is_better=True, icon="⚔️")
def _pokedle(m):
    s = int(m.group(1))
    return s, 8, f"{s} shots"


@game_parser("LoLdle", r"#LoLdle[\s\S]*?in\s+(\d+)\s+shots?",
             lower_is_better=True, icon="🎮")
def _loldle(m):
    s = int(m.group(1))
    return s, 8, f"{s} shots"


@game_parser("Narutodle", r"#Narutodle[\s\S]*?in\s+(\d+)\s+shots?",
             lower_is_better=True, icon="🍥")
def _narutodle(m):
    s = int(m.group(1))
    return s, 8, f"{s} shots"


@game_parser("WhenTaken", r"#WhenTaken[\s\S]*?I scored\s+(\d+)/(\d+)",
             lower_is_better=False, icon="🗓️")
def _whentaken(m):
    s, mx = int(m.group(1)), int(m.group(2))
    return s, mx, f"{s}/{mx}"


@game_parser("Dialed", r"Dialed\s+Daily[\s\S]*?([\d.]+)/50",
             icon="📞")
def _dialed(m):
    s = float(m.group(1))
    return s, 50, f"{s}/50"


@game_parser("Catfishing", r"catfishing\.net\s*\n?\s*#?\d+\s*-\s*(\d+)/(\d+)",
             lower_is_better=False, icon="🐟")
def _catfishing(m):
    s, mx = int(m.group(1)), int(m.group(2))
    return s, mx, f"{s}/{mx}"


@game_parser("Feudle", r"Feudle[\s\S]*?Score:\s*(\d+)/(\d+)",
             icon="📖")
def _feudle(m):
    s, mx = int(m.group(1)), int(m.group(2))
    return s, mx, f"{s}/{mx}"


@game_parser("Doctordle", r"Doctordle\s*#\d+\s*\n([^\n]+)",
             icon="🏥")
def _doctordle(m):
    line = m.group(1)
    g = line.count("🟩")
    r = line.count("🟥")
    b = line.count("⬛")
    total = g + r + b
    if total == 0:
        return None
    return g, total, f"{g}/{total} solved"


@game_parser("TimeGuessr", r"TimeGuessr\s*#\d+\s*([\d,]+)/([\d,]+)",
             icon="⏱️")
def _timeguessr(m):
    s  = int(m.group(1).replace(",", ""))
    mx = int(m.group(2).replace(",", ""))
    return s, mx, f"{s:,}/{mx:,}"


# ═══════════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════════

def _db():
    return sqlite3.connect(DB_PATH)


def init_db():
    with _db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                username    TEXT NOT NULL,
                game        TEXT NOT NULL,
                score       REAL NOT NULL,
                max_score   REAL NOT NULL,
                display     TEXT NOT NULL,
                puzzle_date TEXT NOT NULL,
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(guild_id, user_id, game, puzzle_date)
            );
            CREATE TABLE IF NOT EXISTS config (
                guild_id           TEXT PRIMARY KEY,
                summary_channel_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_results_gd
                ON results(guild_id, puzzle_date);
            CREATE INDEX IF NOT EXISTS idx_results_game
                ON results(guild_id, game);
        """)
    log.info("DB ready  (%s)", DB_PATH)


def save_result(gid, uid, uname, r: GameResult, date):
    with _db() as db:
        db.execute("""
            INSERT INTO results
                (guild_id, user_id, username, game, score, max_score,
                 display, puzzle_date)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(guild_id, user_id, game, puzzle_date) DO UPDATE SET
                score=excluded.score, max_score=excluded.max_score,
                display=excluded.display, username=excluded.username,
                created_at=datetime('now')
        """, (gid, uid, uname, r.game, r.score, r.max_score,
              r.display, date))


def fetch(gid, *, date=None, start=None, end=None, game=None, uid=None):
    q, p = "SELECT * FROM results WHERE guild_id=?", [gid]
    if date:  q += " AND puzzle_date=?";          p.append(date)
    if start: q += " AND puzzle_date>=?";         p.append(start)
    if end:   q += " AND puzzle_date<=?";         p.append(end)
    if game:  q += " AND LOWER(game)=LOWER(?)";  p.append(game)
    if uid:   q += " AND user_id=?";              p.append(uid)
    q += " ORDER BY puzzle_date DESC, game, score"
    with _db() as db:
        db.row_factory = sqlite3.Row
        return db.execute(q, p).fetchall()


# ═══════════════════════════════════════════════════════════════════
#  SCORING HELPERS
# ═══════════════════════════════════════════════════════════════════

def _compute_crowns(rows):
    by_gd: dict[tuple, list] = defaultdict(list)
    for r in rows:
        by_gd[(r["game"], r["puzzle_date"])].append(r)

    user_crowns:   dict[tuple, int]       = defaultdict(int)
    daily_winners: dict[tuple, set[str]]  = {}

    for (game, date), gd_rows in by_gd.items():
        meta = _GAME_META.get(game.lower(), {})
        low  = meta.get("low", False)
        scores = [r["score"] for r in gd_rows]
        best   = min(scores) if low else max(scores)
        winners = set()
        for r in gd_rows:
            if r["score"] == best:
                user_crowns[(r["user_id"], game)] += 1
                winners.add(r["user_id"])
        daily_winners[(game, date)] = winners

    return user_crowns, daily_winners


def _rank_items(items, *, key, reverse=False):
    ordered = sorted(items, key=key, reverse=reverse)
    out, prev_val, prev_rank = [], None, 0
    for i, item in enumerate(ordered):
        val = key(item)
        if val != prev_val:
            prev_rank = i
            prev_val  = val
        medal = MEDALS[prev_rank] if prev_rank < len(MEDALS) else f"`{prev_rank+1}.`"
        out.append((prev_rank, medal, item))
    return out


def _fmt_avg(avg, max_score):
    if max_score >= 1000:
        return f"{avg:,.0f}/{max_score:,.0f}"
    elif max_score == int(max_score):
        return f"{avg:.1f}/{int(max_score)}"
    else:
        return f"{avg:.2f}/{max_score:.0f}"


# ═══════════════════════════════════════════════════════════════════
#  EMBED BUILDERS
# ═══════════════════════════════════════════════════════════════════

def _build_daily_embed(title, rows):
    by_game: dict[str, list] = defaultdict(list)
    for r in rows:
        by_game[r["game"]].append(r)

    embed = discord.Embed(title=title, color=0x5865F2,
                          timestamp=datetime.now(timezone.utc))
    if not by_game:
        embed.description = "No results yet — paste a game share to get started!"
        return embed

    for game_name in sorted(by_game):
        gr   = by_game[game_name]
        meta = _GAME_META.get(game_name.lower(), {})
        low  = meta.get("low", False)
        icon = meta.get("icon", "🎮")

        ranked = _rank_items(gr, key=lambda r: r["score"],
                             reverse=(not low))
        best_score = ranked[0][2]["score"] if ranked else None

        lines = []
        for _rank, medal, r in ranked:
            crown = " 👑" if r["score"] == best_score and len(gr) > 1 else ""
            lines.append(f"{medal} **{r['username']}** — {r['display']}{crown}")

        embed.add_field(name=f"{icon}  {game_name}",
                        value="\n".join(lines), inline=False)
    return embed


def _build_period_embed(title, rows, *, show_detail=False):
    by_game: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by_game[r["game"]][r["user_id"]].append(r)

    embed = discord.Embed(title=title, color=0x5865F2,
                          timestamp=datetime.now(timezone.utc))
    if not by_game:
        embed.description = "No results for this period!"
        return embed

    user_crowns, daily_winners = _compute_crowns(rows)

    for game_name in sorted(by_game):
        users = by_game[game_name]
        meta  = _GAME_META.get(game_name.lower(), {})
        low   = meta.get("low", False)
        icon  = meta.get("icon", "🎮")

        stats = []
        for uid, urows in users.items():
            avg  = sum(r["score"] for r in urows) / len(urows)
            mx   = urows[0]["max_score"]
            best = min(r["score"] for r in urows) if low \
                   else max(r["score"] for r in urows)
            best_disp = next(r["display"] for r in urows if r["score"] == best)
            stats.append(dict(
                uid=uid, name=urows[0]["username"], avg=avg,
                max_score=mx, count=len(urows),
                crowns=user_crowns.get((uid, game_name), 0),
                best=best, best_disp=best_disp, rows=urows))

        ranked = _rank_items(stats, key=lambda p: p["avg"],
                             reverse=(not low))

        lines = []
        for _rank, medal, p in ranked:
            avg_s   = _fmt_avg(p["avg"], p["max_score"])
            crown_s = f" · 👑{p['crowns']}" if p["crowns"] else ""
            lines.append(
                f"{medal} **{p['name']}** — avg {avg_s} "
                f"({p['count']}d{crown_s})")

            if show_detail:
                day_parts = []
                for r in sorted(p["rows"],
                                key=lambda r: r["puzzle_date"]):
                    d = datetime.strptime(r["puzzle_date"], "%Y-%m-%d")
                    lbl = d.strftime("%b %d")
                    won = r["user_id"] in daily_winners.get(
                        (game_name, r["puzzle_date"]), set())
                    day_parts.append(
                        f"{lbl}: {r['display']}{' 👑' if won else ''}")
                lines.append(f"╰ {' · '.join(day_parts)}")

        embed.add_field(name=f"{icon}  {game_name}",
                        value="\n".join(lines) or "—", inline=False)
    return embed


def _add_streaks(embed, gid):
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=90)).isoformat()
    rows = fetch(gid, start=cutoff)

    ud: dict[str, set[str]] = defaultdict(set)
    un: dict[str, str]      = {}
    for r in rows:
        ud[r["user_id"]].add(r["puzzle_date"])
        un[r["user_id"]] = r["username"]

    today   = datetime.now(timezone.utc).date()
    streaks = []
    for uid, raw in ud.items():
        ds = sorted((datetime.strptime(d, "%Y-%m-%d").date()
                      for d in raw), reverse=True)
        if not ds or ds[0] < today - timedelta(days=1):
            continue
        s = 1
        for j in range(1, len(ds)):
            if ds[j] == ds[j-1] - timedelta(days=1):
                s += 1
            else:
                break
        if s >= 2:
            streaks.append((un[uid], s))

    if streaks:
        streaks.sort(key=lambda x: x[1], reverse=True)
        val = "\n".join(
            f"**{n}** — {s} day{'s'*(s!=1)} 🔥" for n, s in streaks)
        embed.add_field(name="🔥  Streaks", value=val, inline=False)


# ═══════════════════════════════════════════════════════════════════
#  DISCORD BOT
# ═══════════════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)


@bot.event
async def on_ready():
    init_db()
    if not daily_summary.is_running():
        daily_summary.start()
    cmds = [c.name for c in bot.commands]
    log.info("✅  %s online  ·  tracking %d games  ·  prefix: '%s'  ·  commands: %s", bot.user, len(_PARSERS), PREFIX, cmds)


@bot.event
async def on_command_error(ctx, error):
    log.info("CMD ERROR in %s: %s", ctx.command, error)


@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return
    log.info("MSG from %s: %s", msg.author.display_name, msg.content[:80])
    results = parse_message(msg.content)
    if results:
        d    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        gid  = str(msg.guild.id)
        uid  = str(msg.author.id)
        name = msg.author.display_name
        for r in results:
            save_result(gid, uid, name, r, d)
            log.info("  📊  %s  ·  %s %s", name, r.game, r.display)
        await msg.add_reaction("📊")
    try:
        await bot.process_commands(msg)
    except Exception as e:
        log.info("COMMAND ERROR: %s", e)


@bot.event
async def on_message_edit(_before, after: discord.Message):
    if after.author.bot or not after.guild:
        return
    results = parse_message(after.content)
    if results:
        d    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        gid  = str(after.guild.id)
        uid  = str(after.author.id)
        name = after.author.display_name
        for r in results:
            save_result(gid, uid, name, r, d)


# ═══════════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════════

PERIODS = {"today", "yesterday", "yday", "week", "month", "all"}


@bot.command(name="lb", aliases=["leaderboard"])
async def cmd_lb(ctx, *, args: str = "today"):
    gid   = str(ctx.guild.id)
    today = datetime.now(timezone.utc).date()

    parts = args.split(maxsplit=1)
    first = parts[0].lower()

    if first in PERIODS:
        period     = first
        game_query = parts[1].strip() if len(parts) > 1 else None
    else:
        period     = "today"
        game_query = args.strip()

    game_name = _match_game(game_query) if game_query else None

    kw: dict     = {}
    single_day   = False

    if period == "today":
        kw["date"] = today.isoformat()
        title = f"📊  Today — {today.strftime('%b %d')}"
        single_day = True
    elif period in ("yesterday", "yday"):
        yd = today - timedelta(days=1)
        kw["date"] = yd.isoformat()
        title = f"📊  Yesterday — {yd.strftime('%b %d')}"
        single_day = True
    elif period == "week":
        mon = today - timedelta(days=today.weekday())
        kw["start"], kw["end"] = mon.isoformat(), today.isoformat()
        title = (f"📊  This Week  "
                 f"({mon.strftime('%b %d')} – {today.strftime('%b %d')})")
    elif period == "month":
        kw["start"] = today.replace(day=1).isoformat()
        kw["end"]   = today.isoformat()
        title = f"📊  {today.strftime('%B %Y')}"
    else:
        title = "📊  All Time"

    if game_name:
        kw["game"] = game_name
        meta = _GAME_META.get(game_name.lower(), {})
        title += f"  ·  {meta.get('icon','🎮')} {game_name}"

    rows = fetch(gid, **kw)

    if single_day:
        embed = _build_daily_embed(title, rows)
    else:
        show_detail = bool(game_name) and period == "week"
        embed = _build_period_embed(title, rows, show_detail=show_detail)

    if single_day and not game_name:
        _add_streaks(embed, gid)

    await ctx.send(embed=embed)


@bot.command(name="games")
async def cmd_games(ctx):
    lines = []
    for p in sorted(_PARSERS, key=lambda p: p["name"]):
        d = "fewer = better" if p["low"] else "higher = better"
        lines.append(f"{p['icon']}  **{p['name']}** — {d}")
    e = discord.Embed(title="🎲  Supported Games", color=0x57F287,
                      description="\n".join(lines))
    e.set_footer(text="Paste a game's share text and I'll track it automatically!")
    await ctx.send(embed=e)


@bot.command(name="mystats", aliases=["stats"])
async def cmd_stats(ctx, member: Optional[discord.Member] = None):
    target = member or ctx.author
    gid    = str(ctx.guild.id)
    uid    = str(target.id)
    rows   = fetch(gid, uid=uid)
    if not rows:
        return await ctx.send(
            f"No results for **{target.display_name}** yet.")

    all_rows      = fetch(gid)
    crowns_map, _ = _compute_crowns(all_rows)

    by_game: dict[str, list] = defaultdict(list)
    for r in rows:
        by_game[r["game"]].append(r)

    total_plays  = 0
    total_crowns = 0

    e = discord.Embed(title=f"📈  {target.display_name}",
                      color=0xFEE75C,
                      timestamp=datetime.now(timezone.utc))

    for gn in sorted(by_game):
        gr   = by_game[gn]
        meta = _GAME_META.get(gn.lower(), {})
        low  = meta.get("low", False)
        icon = meta.get("icon", "🎮")

        avg  = sum(r["score"] for r in gr) / len(gr)
        mx   = gr[0]["max_score"]
        best = min(r["score"] for r in gr) if low \
               else max(r["score"] for r in gr)
        best_d = next(r["display"] for r in gr if r["score"] == best)
        c = crowns_map.get((uid, gn), 0)

        total_plays  += len(gr)
        total_crowns += c

        avg_s   = _fmt_avg(avg, mx)
        crown_s = f" · 👑 {c}" if c else ""
        e.add_field(
            name=f"{icon}  {gn}",
            value=(f"Avg **{avg_s}** · Best **{best_d}** · "
                   f"{len(gr)} plays{crown_s}"),
            inline=False)

    dates = set(r["puzzle_date"] for r in rows)
    e.description = (f"**{total_plays}** plays across **{len(dates)}** days "
                     f"· **{total_crowns}** 👑 total")
    await ctx.send(embed=e)


@bot.command(name="crowns")
async def cmd_crowns(ctx, *, args: str = "all"):
    gid   = str(ctx.guild.id)
    today = datetime.now(timezone.utc).date()

    parts = args.split(maxsplit=1)
    first = parts[0].lower()

    if first in PERIODS:
        period     = first
        game_query = parts[1].strip() if len(parts) > 1 else None
    elif first == "all":
        period     = "all"
        game_query = None
    else:
        period     = "all"
        game_query = args.strip()

    game_name = _match_game(game_query) if game_query else None

    kw: dict = {}
    if period == "today":
        kw["date"] = today.isoformat()
    elif period in ("yesterday", "yday"):
        kw["date"] = (today - timedelta(days=1)).isoformat()
    elif period == "week":
        mon = today - timedelta(days=today.weekday())
        kw["start"], kw["end"] = mon.isoformat(), today.isoformat()
    elif period == "month":
        kw["start"] = today.replace(day=1).isoformat()
        kw["end"]   = today.isoformat()
    if game_name:
        kw["game"] = game_name

    rows = fetch(gid, **kw)
    crowns_map, _ = _compute_crowns(rows)

    names: dict[str, str] = {}
    for r in rows:
        names[r["user_id"]] = r["username"]

    user_agg: dict[str, dict] = defaultdict(
        lambda: {"total": 0, "by_game": defaultdict(int), "name": ""})
    for (uid, gn), c in crowns_map.items():
        user_agg[uid]["total"]        += c
        user_agg[uid]["by_game"][gn]  += c
        user_agg[uid]["name"]          = names.get(uid, "?")

    ranked = _rank_items(list(user_agg.items()),
                         key=lambda x: x[1]["total"], reverse=True)

    title = "👑  Crown Leaderboard"
    if game_name:
        meta = _GAME_META.get(game_name.lower(), {})
        title += f" · {meta.get('icon','🎮')} {game_name}"
    if period != "all":
        title += f" · {period.title()}"

    e = discord.Embed(title=title, color=0xFFD700,
                      timestamp=datetime.now(timezone.utc))
    if not ranked:
        e.description = "No crowns earned yet!"
        return await ctx.send(embed=e)

    lines = []
    for _rank, medal, (uid, data) in ranked[:15]:
        breakdown = " · ".join(
            f"{_GAME_META.get(g.lower(),{}).get('icon','🎮')}{c}"
            for g, c in sorted(data["by_game"].items(),
                               key=lambda x: x[1], reverse=True))
        lines.append(
            f"{medal} **{data['name']}** — **{data['total']}** crowns\n"
            f"╰ {breakdown}")

    e.description = "\n".join(lines)
    await ctx.send(embed=e)


@bot.command(name="setchannel")
@commands.has_permissions(manage_guild=True)
async def cmd_setchannel(ctx, channel: Optional[discord.TextChannel] = None):
    gid = str(ctx.guild.id)
    cid = str(channel.id) if channel else None
    with _db() as db:
        db.execute("""
            INSERT INTO config (guild_id, summary_channel_id) VALUES (?, ?)
            ON CONFLICT(guild_id)
            DO UPDATE SET summary_channel_id = excluded.summary_channel_id
        """, (gid, cid))
    if channel:
        await ctx.send(f"✅  Daily recaps → {channel.mention}")
    else:
        await ctx.send("✅  Daily recaps disabled.")


@bot.command(name="help")
async def cmd_help(ctx):
    e = discord.Embed(
        title="🎲  Zaily Games Leaderboard",
        color=0x5865F2,
        description=(
            "I track daily puzzle games automatically!\n"
            "Just paste your game's share text — I react 📊 when tracked."))
    e.add_field(name="📊  Leaderboards", inline=False, value=(
        f"`{PREFIX}lb` — today\n"
        f"`{PREFIX}lb yesterday`\n"
        f"`{PREFIX}lb week` / `month` / `all`\n"
        f"`{PREFIX}lb wordle` — one game, today\n"
        f"`{PREFIX}lb week wordle` — one game, weekly detail"))
    e.add_field(name="👑  Crowns & Stats", inline=False, value=(
        f"`{PREFIX}crowns` — crown leaderboard\n"
        f"`{PREFIX}crowns week` / `{PREFIX}crowns wordle`\n"
        f"`{PREFIX}mystats` / `{PREFIX}stats @user`"))
    e.add_field(name="⚙️  Setup", inline=False, value=(
        f"`{PREFIX}games` — list supported games\n"
        f"`{PREFIX}setchannel #channel` — auto daily leaderboard at 11 PM ET\n"
        f"`{PREFIX}setchannel` — disable auto-post"))
    e.set_footer(text="Add new games → edit GAME PARSERS in bot.py")
    await ctx.send(embed=e)


# ═══════════════════════════════════════════════════════════════════
#  DAILY RECAP
# ═══════════════════════════════════════════════════════════════════

@tasks.loop(time=dt_time(hour=23, minute=0, tzinfo=ZoneInfo("America/New_York")))
async def daily_summary():
    today = datetime.now(ZoneInfo("America/New_York")).date()
    ds = today.isoformat()
    for guild in bot.guilds:
        gid = str(guild.id)
        with _db() as db:
            db.row_factory = sqlite3.Row
            cfg = db.execute(
                "SELECT summary_channel_id FROM config WHERE guild_id=?",
                (gid,)).fetchone()
        if not cfg or not cfg["summary_channel_id"]:
            continue
        ch = bot.get_channel(int(cfg["summary_channel_id"]))
        if not ch:
            continue
        rows = fetch(gid, date=ds)
        if not rows:
            continue
        embed = _build_daily_embed(
            f"📊  Daily Leaderboard — {today.strftime('%b %d')}", rows)
        _add_streaks(embed, gid)
        await ch.send(embed=embed)
        log.info("Recap sent → %s", guild.name)


# ═══════════════════════════════════════════════════════════════════
#  START
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(
            "Set DAILY_GAMES_BOT_TOKEN  (env var or .env file)")
    bot.run(TOKEN)
