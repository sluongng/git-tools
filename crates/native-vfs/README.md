# native-vfs · library

Owns a native source workspace and projects it through Linux FUSE, macOS NFS or
Windows ProjFS. These use the existing platform providers; protocol tests are not
native macOS/Windows acceptance. Directory and symlink metadata are installed before mounting;
regular files hydrate on demand through the daemon's shared local CAS.

Use private state outside the mount and an opaque workspace ID. Restart preserves
dirty files and uses the durable current base, not a bootstrap reset. Persistent
Windows roots require an exact existing ProjFS binding before nonempty reuse. The virtual
`.jj/native-workspace` marker is an opaque locator; real jj metadata stays outside.

Capture requires quiescent, flushed native users and an authoritative native
frontend. NFS and Windows controller integration currently reject capture;
the Windows provider's NTFS capture groundwork is not yet wired into this API.
Checked checkout uses the provider's publication fence; unsupported providers
return an error, never a raw namespace replacement. Native kernel acceptance is
separate from the portable protocol tests.
