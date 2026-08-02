# Installing / updating the Home Connect plugin on jarvis

The Indigo server (`jarvis.local`) runs the live plugin. There are two distinct
paths — a first install and a subsequent update — and they are **not**
interchangeable.

## First install — double-click only

A brand-new plugin must be registered with Indigo through the UI. Copying the
bundle into the `Plugins/` folder does **not** register it (Indigo only picks up
new bundles via the installer path).

1. Build/obtain the `Home Connect.indigoPlugin` bundle (the folder in this repo,
   or the `.indigoPlugin.zip` from the GitHub release created on merge to
   `main`).
2. On the Mac running Indigo, **double-click** `Home Connect.indigoPlugin`.
   Indigo prompts to install and enable it.
3. Open the plugin's **Configure…** dialog, paste the Home Connect **Client ID**
   (and Client Secret if the application defines one), and click **Authorize**.
   Follow the Device Flow link, or tick **Use the Home Connect simulator** for a
   no-hardware test against the 6 preloaded simulator appliances.
4. Create devices: **Devices → New…**, type **Home Connect**, pick the matching
   model (Dishwasher / Dryer / Washer / Oven / Coffee Maker / Fridge Freezer, or
   the generic *Home Connect Appliance*), then choose the appliance from the
   **Appliance** menu (populated from discovery — authorize first).

## Subsequent updates — copy files + restart

Once the bundle is registered, updates are just file copies followed by a plugin
restart (workspace standard). Over `ssh jarvis.local` (key-based, see workspace
memory `jarvis-ssh-deploy`):

```bash
# From the repo root on your Mac:
DEST="/Library/Application Support/Perceptive Automation/Indigo 2025.2/Plugins/Home Connect.indigoPlugin"
rsync -av --delete "Home Connect.indigoPlugin/" "jarvis.local:$DEST/"
```

Then restart the plugin so the new code loads:

```
mcp__indigo__restart_plugin(plugin_id="com.simons-plugins.homeconnect")
```

Check the Event Log (`mcp__indigo__query_event_log`) for `Home Connect event
stream connected` and the per-appliance discovery lines.

## Notes

- **Dynamic states**: undocumented Home Connect keys surface as extra string
  states on the device automatically (via `getDeviceStateList`). If new states
  appear after an update, that is expected.
- **First-install vs update trap**: if you *copied* a first install instead of
  double-clicking, Indigo shows the bundle but never runs it — double-click to
  register, then future copies work.
- **Off vs disconnected**: each device has a *Treat "disconnected" as Off*
  checkbox. Leave it on for appliances that report power-off as a disconnect;
  turn it off to have the device flag an error state when it drops off the cloud.
