#!/usr/bin/env python3
"""
Zaily Games Leaderboard Bot for Discord — Message History Edition
Monthly reset to prevent backlog accumulation
"""

import re, os, logging, asyncio
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
PREFIX     = os.getenv("BOT_PREFIX", "!zgb ")
RESET_DAY  = int(os.getenv("RESET_DAY", "1"))  # Day of month to reset (default: 1st)

log = logging.getLogger("zailygames")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")

MEDALS = ["🥇", "🥈", "🥉"]


# ═══════════════════════════════════════════════════════════════════
#  IN-MEMORY STORAGE (Monthly reset)
# ═══════════════════════════════════════════════════════════════════

class ResultsStore:
    """In-memory storage with monthly reset"""
    
    def __init__(self):
        # Structure: {(guild_id, user_id, game, date): GameResult}
        self.results: dict[tuple, dict] = {}
        self.current_month: int = datetime.now(timezone.utc).month
    
    def check_reset(self):
        """Reset storage if we've entered a new month"""
        now = datetime.now(timezone.utc)
        if now.month != self.current_month and now.day >= RESET_DAY:
            old_month = self.current_month
            self.results.clear()
            self.current_month = now.month
            log.info("🗑️  Monthly reset triggered: cleared %d results from month %d", 
                     len(self.results), old_month)
            return True
        return False
    
    def save(self, guild_id: str, user_id: str, username: str, 
             game_result, date_str: str):
        """Save a result, overwriting any existing entry for same user/game/date"""
        key = (guild_id, user_id, game_result.game, date_str)
        self.results[key] = {
            "guild_id": guild_id,
            "user_id": user_id,
            "username": username,
            "game": game_result.game,
            "score": game_result.score,
            "max_score": game_result.max_score,
            "display": game_result.display,
            "puzzle_date": date_str,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
    
    def fetch(self, guild_id: str, *, date=None, start=None, end=None, 
              game=None, uid=None):
        """Fetch results matching criteria"""
        rows = []
        for key, r in self.results.items():
            if r["guild_id"] != guild_id:
                continue
            if date and r["puzzle_date"] != date:
                continue
            if start and r["puzzle_date"] < start:
                continue
            if end and r["puzzle_date"] > end:
                continue
            if game and r["game"].lower() != game.lower():
                continue
            if uid and r["user_id"] != uid:
                continue
            rows.append(r)
        # Sort by date desc, game, score
        meta_low = {p["name"].lower(): p["low"] for p in _PARSERS}
        rows.sort(key=lambda r: (
            r["puzzle_date"], 
            r["game"], 
            r["score"] if meta_low.get(r["game"].lower(), False) else -r["score"]
        ), reverse=True)
        return rows
    
    def get_all(self, guild_id: str):
        """Get all results for a guild"""
        return [r for r in self.results.values() if r["guild_id"] == guild_id]


# Global storage instance
store = ResultsStore()


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
#  MESSAGE HISTORY FETCHING
# ═══════════════════════════════════════════════════════════════════

async def fetch_message_history(channel: discord.TextChannel, 
                                after: Optional[datetime] = None,
                                limit: int = 1000) -> list[discord.Message]:
    """Fetch messages from channel history"""
    messages = []
    try:
        # Discord.py handles pagination automatically
        async for msg in channel.history(limit=limit, after=after, oldest_first=False):
            messages.append(msg)
    except discord.HTTPException as e:
        log.error("Error fetching history: %s", e)
    return messages


async def build_results_from_history(channel: discord.TextChannel,
                                      days: int = 30) -> list[dict]:
    """Build results list by fetching and parsing message history"""
    after = datetime.now(timezone.utc) - timedelta(days=days)
    messages = await fetch_message_history(channel, after=after)
    
    results = []
    for msg in messages:
        if msg.author.bot:
            continue
        
        game_results = parse_message(msg.content)
        if game_results:
            date_str = msg.created_at.strftime("%Y-%m-%d")
            for gr in game_results:
                results.append({
                    "guild_id": str(channel.guild.id),
                    "user_id": str(msg.author.id),
                    "username": msg.author.display_name,
                    "game": gr.game,
                    "score": gr.score,
                    "max_score": gr.max_score,
                    "display": gr.display,
                    "puzzle_date": date_str,
                    "created_at": msg.created_at.isoformat(),
                    "message_id": msg.id
                })
    
    return results


def deduplicate_results(results: list[dict]) -> list[dict]:
    """Keep only the latest result for each user/game/date combination"""
    # Sort by created_at descending
    sorted_results = sorted(results, 
                          key=lambda r: r.get("created_at", ""), 
                          reverse=True)
    
    seen = set()
    unique = []
    for r in sorted_results:
        key = (r["guild_id"], r["user_id"], r["game"], r["puzzle_date"])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    
    return unique


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


def _add_streaks(embed, rows):
    """Calculate and add streaks section to embed"""
    # Filter to last 90 days
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=90)).isoformat()
    recent_rows = [r for r in rows if r["puzzle_date"] >= cutoff]
    
    ud: dict[str, set[str]] = defaultdict(set)
    un: dict[str, str]      = {}
    for r in recent_rows:
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

# Store for configured channels (in-memory only now)
config_store: dict[str, Optional[int]] = {}  # guild_id -> channel_id


@bot.event
async def on_ready():
    # Check for monthly reset
    if store.check_reset():
        log.info("🗑️  Monthly storage reset performed")
    
    if not daily_summary.is_running():
        daily_summary.start()
    if not monthly_reset_check.is_running():
        monthly_reset_check.start()
    
    cmds = [c.name for c in bot.commands]
    log.info("✅  %s online  ·  tracking %d games  ·  prefix: '%s'  ·  commands: %s", 
             bot.user, len(_PARSERS), PREFIX, cmds)


@bot.event
async def on_command_error(ctx, error):
    log.info("CMD ERROR in %s: %s", ctx.command, error)


@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return
    
    # Check for monthly reset when processing messages
    store.check_reset()
    
    log.info("MSG from %s: %s", msg.author.display_name, msg.content[:80])
    results = parse_message(msg.content)
    if results:
        d    = msg.created_at.strftime("%Y-%m-%d")
        gid  = str(msg.guild.id)
        uid  = str(msg.author.id)
        name = msg.author.display_name
        for r in results:
            store.save(gid, uid, name, r, d)
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
    
    # Check for monthly reset
    store.check_reset()
    
    results = parse_message(after.content)
    if results:
        d    = after.created_at.strftime("%Y-%m-%d")
        gid  = str(after.guild.id)
        uid  = str(after.author.id)
        name = after.author.display_name
        for r in results:
            store.save(gid, uid, name, r, d)


# ═══════════════════════════════════════════════════════════════════
#  HISTORY SYNC HELPERS
# ═══════════════════════════════════════════════════════════════════

async def sync_history_to_store(channel: discord.TextChannel, days: int = 30):
    """Fetch message history and populate the store"""
    after = datetime.now(timezone.utc) - timedelta(days=days)
    message_count = 0
    result_count = 0
    
    try:
        async for msg in channel.history(limit=1000, after=after, oldest_first=False):
            if msg.author.bot:
                continue
            
            results = parse_message(msg.content)
            if results:
                date_str = msg.created_at.strftime("%Y-%m-%d")
                gid = str(channel.guild.id)
                uid = str(msg.author.id)
                name = msg.author.display_name
                
                for r in results:
                    store.save(gid, uid, name, r, date_str)
                    result_count += 1
            message_count += 1
    except discord.HTTPException as e:
        log.error("Error fetching history: %s", e)
    
    log.info(f"Synced {message_count} messages, found {result_count} game results from last {days} days")
    return result_count


# ═══════════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════════

PERIODS = {"today", "yesterday", "yday", "week", "month", "all"}


@bot.command(name="sync")
async def cmd_sync(ctx, days: int = 30):
    """Manually sync message history to populate the store"""
    await ctx.send(f"🔄 Syncing last {days} days of message history...")
    count = await sync_history_to_store(ctx.channel, days)
    await ctx.send(f"✅ Found {count} game results!")


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

    # First try in-memory store
    rows = store.fetch(gid, **kw)
    
    # If no results, sync from message history
    if not rows and isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("🔄 Fetching message history...", delete_after=3)
        days_to_sync = 1 if single_day else (7 if period == "week" else 30)
        await sync_history_to_store(ctx.channel, days=days_to_sync)
        rows = store.fetch(gid, **kw)

    if single_day:
        embed = _build_daily_embed(title, rows)
    else:
        show_detail = bool(game_name) and period == "week"
        embed = _build_period_embed(title, rows, show_detail=show_detail)

    if single_day and not game_name:
        # Get all rows for streak calculation
        all_rows = store.get_all(gid)
        _add_streaks(embed, all_rows)

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
    
    # Fetch from in-memory store
    rows = store.fetch(gid, uid=uid)
    
    # If no results, sync from history first
    if not rows and isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("🔄 Fetching your history...", delete_after=3)
        await sync_history_to_store(ctx.channel, days=30)
        rows = store.fetch(gid, uid=uid)
    
    if not rows:
        return await ctx.send(
            f"No results for **{target.display_name}** yet.")

    all_rows      = store.get_all(gid)
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

    # Fetch from in-memory store
    rows = store.fetch(gid, **kw)
    
    # If no results, sync from history first
    if not rows and isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("🔄 Fetching leaderboard history...", delete_after=3)
        days_to_sync = 1 if period == "today" else (7 if period == "week" else 30)
        await sync_history_to_store(ctx.channel, days=days_to_sync)
        rows = store.fetch(gid, **kw)
    
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
    config_store[gid] = channel.id if channel else None
    
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
        f"`{PREFIX}setchannel` — disable auto-post\n\n"
        f"🗑️  **Auto-reset**: Leaderboards reset monthly on day {RESET_DAY}"))
    e.set_footer(text="Add new games → edit GAME PARSERS in bot.py")
    await ctx.send(embed=e)


# ═══════════════════════════════════════════════════════════════════
#  DAILY RECAP & MONTHLY RESET
# ═══════════════════════════════════════════════════════════════════

@tasks.loop(time=dt_time(hour=23, minute=0, tzinfo=ZoneInfo("America/New_York")))
async def daily_summary():
    """Post daily leaderboard recap to configured channels"""
    today = datetime.now(ZoneInfo("America/New_York")).date()
    ds = today.isoformat()
    
    for guild in bot.guilds:
        gid = str(guild.id)
        channel_id = config_store.get(gid)
        
        if not channel_id:
            continue
            
        ch = bot.get_channel(channel_id)
        if not ch:
            continue
            
        # Fetch from in-memory store
        rows = store.fetch(gid, date=ds)
        if not rows:
            continue
            
        embed = _build_daily_embed(
            f"📊  Daily Leaderboard — {today.strftime('%b %d')}", rows)
        
        all_rows = store.get_all(gid)
        _add_streaks(embed, all_rows)
        
        await ch.send(embed=embed)
        log.info("Recap sent → %s", guild.name)


@tasks.loop(time=dt_time(hour=0, minute=5, tzinfo=ZoneInfo("America/New_York")))
async def monthly_reset_check():
    """Check and perform monthly reset"""
    if store.check_reset():
        log.info("🗑️  Scheduled monthly reset completed")
        # Optionally announce reset in configured channels
        for guild in bot.guilds:
            gid = str(guild.id)
            channel_id = config_store.get(gid)
            if channel_id:
                ch = bot.get_channel(channel_id)
                if ch:
                    try:
                        await ch.send("🗑️ **Monthly Reset**: Leaderboard data has been cleared for the new month!")
                    except:
                        pass


# ═══════════════════════════════════════════════════════════════════
#  START
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(
            "Set DAILY_GAMES_BOT_TOKEN  (env var or .env file)")
    bot.run(TOKEN)
