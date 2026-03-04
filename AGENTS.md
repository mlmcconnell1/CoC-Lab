# Agent Instructions

This project uses `.beads/` for issue tracking. Two CLI tools can manage beads: **br** (beads_rust, preferred) and **bd** (beads Python). At session start, detect which is available:

```bash
command -v br && BR=br || { command -v bd && BR=bd || echo "No beads CLI found"; }
```

Use `$BR` (or just the resolved command) for all beads operations below. Prefer **br** when both are installed.

**Note:** `br` is non-invasive and never executes git commands. After `br sync --flush-only`, you must manually run `git add .beads/ && git commit`. The `bd sync` command commits and pushes automatically.

## Quick Reference

| Action | br (preferred) | bd (fallback) |
|--------|---------------|---------------|
| Onboard | `br onboard` | — |
| List issues | `br list` | `bd list --status=open` |
| Find ready work | `br ready` | `bd ready` |
| Show issue | `br show <id>` | `bd show <id>` |
| Create issue | `br create` | `bd create --title="..." --type=task --priority=2` |
| Claim work | `br update <id> --status in_progress` | `bd update <id> --status=in_progress` |
| Close issue | `br close <id>` | `bd close <id> --reason="Completed"` |
| Sync to disk | `br sync --flush-only` | `bd sync` |

## Adding Beads (Problem Noticed)

If you identify a problem in the code, even incidentally while working on something else, add a bead to make sure it is addressed later.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   # If using br:
   br sync --flush-only
   git add .beads/
   git commit -m "sync beads"
   # If using bd:
   # bd sync              # (commits and pushes automatically)
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Key Concepts

- **Dependencies**: Issues can block other issues. `ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
