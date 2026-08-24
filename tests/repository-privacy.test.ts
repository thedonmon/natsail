import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const repositoryRoot = fileURLToPath(new URL('..', import.meta.url))
const pathSeparator = String.raw`[/\\]`
const forbiddenPaths = [
  {
    label: 'macOS user home',
    pattern: new RegExp(`${pathSeparator}Users${pathSeparator}[^/\\\\s]+${pathSeparator}`, 'i'),
  },
  {
    label: 'Linux user home',
    pattern: new RegExp(`${pathSeparator}home${pathSeparator}[^/\\\\s]+${pathSeparator}`, 'i'),
  },
  {
    label: 'personal source folder',
    pattern: new RegExp(`${['Source', 'Code'].join('')}${pathSeparator}work`, 'i'),
  },
]

describe('repository privacy', () => {
  it('does not contain personal absolute paths in repository files', () => {
    const trackedFiles = execFileSync(
      'git',
      ['ls-files', '--cached', '--others', '--exclude-standard', '-z'],
      {
        cwd: repositoryRoot,
        encoding: 'utf8',
      }
    )
      .split('\0')
      .filter(Boolean)
    const violations: string[] = []

    for (const file of trackedFiles) {
      const contents = readFileSync(resolve(repositoryRoot, file))
      const text = contents.toString('utf8')
      for (const forbidden of forbiddenPaths) {
        if (forbidden.pattern.test(text)) {
          violations.push(`${file}: ${forbidden.label}`)
        }
      }
    }

    expect(violations, 'Remove personal filesystem paths before publishing').toEqual([])
  })
})
