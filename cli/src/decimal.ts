const DECIMAL_PATTERN = /^-?\d+(\.\d+)?$/;

type Scaled = { units: bigint; scale: number };

function toScaled(text: string, scale: number): bigint {
  const negative = text.startsWith("-");
  const [whole = "0", fraction = ""] = (negative ? text.slice(1) : text).split(".");
  const units = BigInt(whole + fraction.padEnd(scale, "0"));
  return negative ? -units : units;
}

function fromScaled({ units, scale }: Scaled): string {
  const negative = units < 0n;
  const digits = (negative ? -units : units).toString().padStart(scale + 1, "0");
  const whole = digits.slice(0, digits.length - scale);
  const fraction = digits.slice(digits.length - scale).replace(/0+$/, "");
  const sign = negative ? "-" : "";
  return fraction === "" ? `${sign}${whole}` : `${sign}${whole}.${fraction}`;
}

// USD totals are summed exactly so the proof summary matches the API's NUMERIC strings.
export function sumDecimalStrings(values: readonly string[]): string {
  const valid = values.filter((value) => DECIMAL_PATTERN.test(value));
  const scale = valid.reduce((largest, value) => Math.max(largest, value.split(".")[1]?.length ?? 0), 0);
  const units = valid.reduce((total, value) => total + toScaled(value, scale), 0n);
  return fromScaled({ units, scale });
}
