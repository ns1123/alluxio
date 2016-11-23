<#--
The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
(the "License"). You may not use this work except in compliance with the License, which is
available at www.apache.org/licenses/LICENSE-2.0

This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied, as more fully set forth in the License.

See the NOTICE file distributed with this work for information regarding copyright ownership.
-->

<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->

<#global newLineIndented = ",\n    "/>

<#function toJsonPair key value>
  <#return "\"" + (key?json_string) + "\": \"" + (value?json_string) + "\""/>
</#function>

<#function licenseFormat licenses>
  <#assign result = ""/>
  <#list licenses as license>
    <#assign result = result + toJsonPair("license" + (license_index), license)/>
    <#if license_has_next>
      <#assign result = result + newLineIndented>
    <#else>
      <#assign result = result + ",">
    </#if>
  </#list>
  <#return result>
</#function>

<#function artifactFormat p>
  <#assign result = ""/>
  <#assign result = result + toJsonPair("name", (p.name?json_string))/>
  <#assign result = result + newLineIndented/>
  <#assign result = result + toJsonPair("version", (p.version?json_string))/>
  <#assign result = result + newLineIndented/>
  <#assign result = result + toJsonPair("url", (p.url!""?json_string))/>
  <#return result>
</#function>

<#if dependencyMap?size == 0>
The project has no dependencies.
<#else>
List of ${dependencyMap?size} third-party dependencies.
[
  <#list dependencyMap as e>
  {
    <#assign project = e.getKey()/>
    <#assign licenses = e.getValue()/>
    ${licenseFormat(licenses)}
    ${artifactFormat(project)}
    <#if e_has_next>
  },
    <#else>
  }
    </#if>
  </#list>
]
</#if>
