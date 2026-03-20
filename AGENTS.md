# AGENTS.md

This repository is the public distribution package for the OpenClaw Facebook Ads Spy plugin.

## Purpose

- expose the installable OpenClaw plugin surface
- expose the minimum public docs required to install the plugin into an existing OpenClaw runtime
- stay small, stable, and easy to update from the private development source

## Source of truth

The development source of truth is a separate private repository.

Do not turn this repository into the main development workspace.

## What belongs here

- installable plugin files
- public setup docs
- public examples
- public agent-facing markdown such as this file

## What does not belong here

- private RFCs
- internal reports
- artifacts
- evals
- chat runs
- beta-only operational notes

## Sync rule

If the upstream private plugin gains a new file, dependency, environment requirement, or setup step, update the export manifest and the public docs in the private source first, then resync this public repository.
