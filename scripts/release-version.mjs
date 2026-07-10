export function selectReleaseVersion({ latestTag = "", packageVersions, requestedVersion = "" }) {
  if (!Array.isArray(packageVersions) || packageVersions.length === 0) {
    throw new Error("At least one package version is required to select a release version.");
  }

  for (const version of packageVersions) validateVersion(version);

  const highestPackageVersion = maxVersion(packageVersions);
  const tagVersion = latestTag ? latestTag.replace(/^v/, "") : "";
  if (tagVersion) validateVersion(tagVersion);

  const baselineVersion = maxVersion([highestPackageVersion, ...(tagVersion ? [tagVersion] : [])]);
  const version = requestedVersion || bumpPatch(baselineVersion);
  validateVersion(version);

  if (compareVersions(version, baselineVersion) <= 0) {
    throw new Error(
      `Release version ${version} must be greater than the current release baseline ${baselineVersion} ` +
        `(highest package ${highestPackageVersion}${tagVersion ? `, latest tag ${latestTag}` : ""}).`,
    );
  }

  return { baselineVersion, highestPackageVersion, version };
}

export function bumpPatch(version) {
  validateVersion(version);
  const parts = version.split(".").map(Number);
  parts[2] += 1;
  return parts.join(".");
}

export function compareVersions(a, b) {
  validateVersion(a.replace(/^v/, ""));
  validateVersion(b.replace(/^v/, ""));
  const left = a.replace(/^v/, "").split(".").map(Number);
  const right = b.replace(/^v/, "").split(".").map(Number);
  for (let index = 0; index < 3; index += 1) {
    if (left[index] !== right[index]) return left[index] - right[index];
  }
  return 0;
}

export function validateVersion(version) {
  if (!/^\d+\.\d+\.\d+$/.test(version)) throw new Error(`Invalid semver version: ${version}`);
}

function maxVersion(versions) {
  return [...versions].sort(compareVersions).at(-1);
}
