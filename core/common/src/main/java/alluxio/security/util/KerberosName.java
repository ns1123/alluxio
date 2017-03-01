/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.util;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Util class to parse and manipulate Kerberos principal name.
 *
 * A typical Kerberos principal name has the following format:
 *   [serviceName]/[hostname]@[realm]
 * If the [hostname] is empty, it will be [serviceName]@[realm].
 */
@ThreadSafe
public final class KerberosName {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosName.class);
  // regexp pattern for a valid Kerberos principal name.
  private static final Pattern NAME_PATTERN = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");
  // regexp pattern for a valid auth_to_local rule.
  private static final Pattern RULE_PATTERN = Pattern.compile(
      "\\s*((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?(s/([^/]*)/([^/]*)/(g)?)?))/?(L)?");

  private static List<Rule> sRules;
  private static String sDefaultRealm;

  private final String mServiceName;
  private final String mHostName;
  private final String mRealm;

  /**
   * Gets the rules. If rules are not yet configured, initialize the rules.
   *
   * @return the list of configured rules
   */
  public static List<Rule> getRules() {
    if (sDefaultRealm == null) {
      synchronized (KerberosName.class) {
        if (sDefaultRealm == null) {
          // Initialize default realm and rules.
          try {
            sDefaultRealm = KerberosUtils.getDefaultRealm();
          } catch (Exception e) {
            LOG.debug("krb5 configuration can not be found. Setting default realm to empty.");
            sDefaultRealm = "";
          }
          String rulesString = Configuration.get(PropertyKey.SECURITY_KERBEROS_AUTH_TO_LOCAL);
          setRules(rulesString);
        }
      }
    }
    return sRules;
  }

  /**
   * Sets the rules. This should only be used for testing purpose.
   *
   * @param rulesString the rules string
   */
  public static void setRulesForTesting(String rulesString) {
    synchronized (KerberosName.class) {
      setRules(rulesString);
    }
  }

  private static void setRules(String rulesString) {
    sRules = (rulesString != null) ? parseRules(rulesString) : null;
  }

  /**
   * Parses the rules in string format to a list of rules.
   *
   * @param rulesString input rules in string format
   * @return the list of rules
   */
  private static List<Rule> parseRules(String rulesString) {
    List<Rule> retval = new ArrayList<>();
    String remaining = rulesString.trim();
    while (remaining.length() > 0) {
      Matcher matcher = RULE_PATTERN.matcher(remaining);
      if (!matcher.lookingAt()) {
        throw new IllegalArgumentException("Invalid rule: " + remaining);
      }
      if (matcher.group(2) != null) {
        retval.add(new Rule());
      } else {
        retval.add(new Rule(Integer.parseInt(matcher.group(4)) /* components */,
            matcher.group(5), /* format */
            matcher.group(7), /* match */
            matcher.group(9), /* from */
            matcher.group(10), /* to */
            "g".equals(matcher.group(11)) /* repeat */,
            "L".equals(matcher.group(12)) /* tolower */));
      }
      remaining = remaining.substring(matcher.end());
    }
    return retval;
  }

  /**
   * Constructs a KerberosName with a principal name string.
   *
   * @param name Kerberos principal name
   */
  public KerberosName(String name) {
    Matcher match = NAME_PATTERN.matcher(name);
    if (!match.matches()) {
      if (name.contains("@")) {
        throw new IllegalArgumentException("Malformed Kerberos name: " + name);
      }
      mServiceName = name;
      mHostName = null;
      mRealm = null;
    } else {
      mServiceName = match.group(1);
      mHostName = match.group(3);
      mRealm = match.group(4);
    }
  }

  /**
   * @return the string format of the Kerberos name
   */
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(mServiceName);
    if (mHostName != null) {
      result.append('/');
      result.append(mHostName);
    }

    if (mRealm != null) {
      result.append('@');
      result.append(mRealm);
    }

    return result.toString();
  }

  /**
   * @return the service name
   */
  public String getServiceName() {
    return mServiceName;
  }

  /**
   * @return the hostname
   */
  public String getHostName() {
    return mHostName;
  }

  /**
   * @return the realm
   */
  public String getRealm() {
    return mRealm;
  }

  /**
   * Get the translation of the principal name into an operating system user name.
   * Each rule in the rule set is processed in order. In other words, when a match is found,
   * the processing stops and returns the generated translation.
   *
   * @return the short name
   * @throws IOException if failed to get the short name
   */
  public String getShortName() throws IOException {
    String[] params;
    if (mHostName == null) {
      if (mRealm == null) {
        // If only service name is present, no need to apply auth_to_local rules.
        return mServiceName;
      }
      params = new String[]{mRealm, mServiceName};
    } else {
      params = new String[]{mRealm, mServiceName, mHostName};
    }
    List<Rule> rules = getRules();
    for (Rule r : rules) {
      String retval = r.apply(params);
      if (retval != null) {
        return retval;
      }
    }
    throw new IOException("No rules applied to " + toString());
  }

  /**
   * A rule to translate Kerberos principal to local operating system user.
   *
   * Each rule has one of the following formats:
   *   DEFAULT
   *   RULE:[n:string](regexp)s/pattern/replacement/
   *   RULE:[n:string](regexp)s/pattern/replacement/g
   *   RULE:[n:string](regexp)s/pattern/replacement//L
   *   RULE:[n:string](regexp)s/pattern/replacement/g/L
   *
   * Meaning for each component:
   * [n:string] indicates a matching rule. The first part n is the number of expected components
   *     in the Kerberos principal (excluding the realm). n must be 1 or 2. A value of 1 specifies
   *     principal names that have a single component (for example, alluxio), and 2 specifies
   *     principal names that have two components (for example, alluxio/fqdn). The second part
   *     string specifies a pattern for translating the principal component(s) and the realm into
   *     a short name. The variable $0 translates the realm, $1 translates the first component,
   *     and $2 translates the second component.
   * (regexp) indicates a matching rule on the value generated by the [n:string] clause
   *     If this regular expression (regexp) matches, then the replacement expression is invoked.
   *     A rule matches only if the specified regular expression matches the entire translated
   *     short name from the principal translation.
   * s/pattern/replacement/
   *     The replacement expression to use to generate a value that is to be used as the
   *     local user account.
   * g If g is specified af the last /, the replacements will occur for
   *     every match in the value, else only the first match is processed.
   * /L If /L is specified, the translated result are updated to be all lower case. By default,
   *     translations based on rules are done maintaining the case of the input principal.
   */
  @ThreadSafe
  private static class Rule {
    private final boolean mIsDefault;
    private final int mComponents;
    private final String mFormat;
    private final Pattern mMatch;
    private final Pattern mFrom;
    private final String mTo;
    private final boolean mRepeat;
    private final boolean mToLowerCase;

    public Rule() {
      mIsDefault = true;
      mComponents = 0;
      mFormat = null;
      mMatch = null;
      mFrom = null;
      mTo = null;
      mRepeat = false;
      mToLowerCase = false;
    }

    public Rule(int components, String format, String match, String from, String to, boolean repeat,
        boolean toLowerCase) {
      mIsDefault = false;
      mComponents = components;
      mFormat = format;
      mMatch = match == null ? null : Pattern.compile(match);
      mFrom = from == null ? null : Pattern.compile(from);
      mTo = to;
      mRepeat = repeat;
      mToLowerCase = toLowerCase;
    }

    private String replaceParameters(String format, String[] params) throws IOException {
      final Pattern parameterPattern = Pattern.compile("([^$]*)(\\$(\\d*))?");
      Matcher match = parameterPattern.matcher(format);
      int start = 0;
      StringBuilder retval = new StringBuilder();
      while (start < format.length() && match.find(start)) {
        retval.append(match.group(1));
        String paramNum = match.group(3);
        if (paramNum != null) {
          try {
            int num = Integer.parseInt(paramNum);
            if (num < 0 || num > params.length) {
              throw new IOException(
                  String.format("In from pattern %s index %d is outside of the valid range 0 to %d",
                  mFrom, num, params.length - 1));
            }
            retval.append(params[num]);
          } catch (NumberFormatException e) {
            throw new IOException("Bad format in username mapping in " + paramNum, e);
          }
        }
        start = match.end();
      }
      return retval.toString();
    }

    private String replaceSubstitution(String base, Pattern from, String to, boolean repeat) {
      Matcher match = from.matcher(base);
      if (repeat) {
        return match.replaceAll(to);
      } else {
        return match.replaceFirst(to);
      }
    }

    public String apply(String[] params) throws IOException {
      String retval = null;
      if (mIsDefault) {
        if (sDefaultRealm.equals(params[0])) {
          retval = params[1];
        }
      } else if (params.length - 1 == mComponents) {
        String base = replaceParameters(mFormat, params);
        if (mMatch == null || mMatch.matcher(base).matches()) {
          if (mFrom == null) {
            retval = base;
          } else {
            retval = replaceSubstitution(base, mFrom, mTo, mRepeat);
          }
        }
      }
      final Pattern nonSimplePattern = Pattern.compile("[/@]");
      if (retval != null && nonSimplePattern.matcher(retval).find()) {
        throw new IOException("Non simple name " + retval + " after auth_to_local rule " + this);
      }
      if (mToLowerCase && retval != null) {
        retval = retval.toLowerCase(Locale.ENGLISH);
      }
      return retval;
    }

    @Override
    public String toString() {
      if (mIsDefault) {
        return "DEFAULT";
      }
      StringBuilder buf = new StringBuilder();
      buf.append("RULE:[");
      buf.append(mComponents);
      buf.append(':');
      buf.append(mFormat);
      buf.append(']');
      if (mMatch != null) {
        buf.append('(');
        buf.append(mMatch);
        buf.append(')');
      }
      if (mFrom != null && mTo != null) {
        buf.append("s/");
        buf.append(mFrom);
        buf.append('/');
        buf.append(mTo);
        buf.append('/');
        if (mRepeat) {
          buf.append('g');
        }
        if (mToLowerCase) {
          buf.append("/L");
        }
      }
      return buf.toString();
    }
  }
}
