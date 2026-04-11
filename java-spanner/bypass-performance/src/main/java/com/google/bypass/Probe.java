package com.google.bypass;

import static java.util.Arrays.stream;

import com.google.cloud.spanner.StructReader;
import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.Random;

/** Generic probe interface. */
public interface Probe {
  /** Type of different probe workloads. */
  enum Type {
    READ_WRITE,
    RR_OCC_DML,
    DML,
    BLIND_DML,
    MULTI_BLIND_DML,
    STRONG_QUERY,
    MULTI_USE_RO_QUERY,
    STALE_QUERY,
    STALE_READ,
    STRONG_READ,
    YCSB_FIXED_READ,
    WRITE,
    WRITE_NO_RP;

    static Optional<Type> find(String probeType) {
      return stream(Type.values())
          .filter(t -> Ascii.toLowerCase(t.name()).equals(probeType))
          .findAny();
    }
  }

  String TABLE = "T";
  ImmutableList<String> COLUMNS = ImmutableList.of("Key", "Value");

  String getName();

  void probe();

  static void validateRows(StructReader iter) {}

  static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  public static String generateRandomString(int length) {
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      int randomIndex = random.nextInt(CHARACTERS.length());
      sb.append(CHARACTERS.charAt(randomIndex));
    }
    return sb.toString();
  }
}
