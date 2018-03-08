<?xml version="1.0" encoding="ISO-8859-1"?>
<!--<?xml version="1.0" encoding="utf-8"?>
<?xml-stylesheet type="text/xsl" href="Documentation.xsl" ?>-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


  <xsl:output
      method="html" indent="yes"
      omit-xml-declaration="yes"/>

  <!-- DOCUMENT TEMPLATE -->
  <!-- Format the whole document as a valid HTML document -->
  <xsl:template match="/">

    <html>
      <head>
        <meta charset="utf-8"/>
        <meta http-equiv="X-UA-Compatible" content="chrome=1"/>
        <title>GitHub Pages Midnight Theme by mattgraham</title>
        <link rel="stylesheet" href="styles/styles.css"/>
        <link rel="stylesheet" href="styles/pygment_trac.css"/>
        <script src="scripts/jquery-3.3.1.min.js"></script>
        <script src="javascripts/respond.js"></script>

        <meta name="viewport" content="width=device-width, initial-scale=1, user-scalable=no"/>

      </head>
      <body>

        <div id="header">
          <nav>
            <li class="fork">
              <a href="https://github.com/electricessence/Open.Database.Extensions">Fork On GitHub</a>
            </li>
            <li class="downloads">
              <a href="https://www.nuget.org/packages/Open.Database.Extensions/">Nuget</a>
            </li>
            <li class="title">Install</li>
          </nav>
        </div>
        <!-- end header -->

        <div class="wrapper">

          <section>
            <xsl:apply-templates select="//assembly"/>

          </section>
        </div>



        <a class="top" href="#top">Top</a>

      </body>
    </html>



  </xsl:template>

  <!-- ASSEMBLY TEMPLATE -->
  <!-- For each Assembly, display its name and then its member types -->
  <xsl:template match="assembly">
    <div id="title">
      <h1>
        <xsl:value-of select="name"/>
      </h1>
      <p>Simplify your .NET/ADO data access. </p>
      <hr/>
      <span class="credits left">
        Project maintained by <a href="https://github.com/electricessence">electricessence</a>
      </span>
      <span class="credits right">
        Hosted on GitHub Pages &#8212; Theme by <a href="http://twitter.com/#!/michigangraham">mattgraham</a>
      </span>
    </div>

    <fieldset class="index">
      <legend>Class Index</legend>
      <xsl:apply-templates select="//member[contains(@name,'T:')]" mode="index"/>
    </fieldset>

    <xsl:apply-templates select="//member[contains(@name,'T:')]" mode="memberIndex"/>

    <xsl:apply-templates select="//member[contains(@name,'T:')]"/>
  </xsl:template>



  <!-- TYPE TEMPLATE -->
  <!-- Loop through member types and display their properties and methods -->
  <xsl:template match="//member[contains(@name,'T:')]" mode="index">
    <div>
      <a>
        <xsl:attribute name="href">
          #<xsl:apply-templates select="." mode="indexname"/>
        </xsl:attribute>
        <xsl:apply-templates select="." mode="fullname"/>
      </a>
    </div>
  </xsl:template>

  <xsl:template match="member" mode="indexname">
    <xsl:value-of select="substring-after(substring-after(substring-after(@name, '.'), '.'), '.')"/>
  </xsl:template>

  <xsl:template match="member" mode="fullname">

    <xsl:value-of select="substring-before(substring-after(substring-after(substring-after(@name, '.'), '.'), '.'),'`')"/>

    <xsl:variable name="typeParams"><xsl:for-each select="typeparam">, <xsl:value-of select="@name"/></xsl:for-each></xsl:variable>

    <xsl:if test="string-length($typeParams) &gt; 0">&lt;<xsl:value-of select="substring($typeParams,3)" />&gt;</xsl:if>
  </xsl:template>

  <!-- TYPE TEMPLATE -->
  <!-- Loop through member types and display their properties and methods -->
  <xsl:template match="//member[contains(@name,'T:')]" mode="memberIndex">

    <!-- Get the type's fully qualified name without the T: prefix -->
    <xsl:variable name="FullMemberName" select="substring-after(@name, ':')"/>
    <div>
      Name: <xsl:value-of select="@name"/>
    </div>
    <div>
      Full Member Name: <xsl:value-of select="$FullMemberName"/>
    </div>

    <!-- Display the type's name and information -->
    <a class="marker">
      <xsl:attribute name="name">
        <xsl:apply-templates select="." mode="indexname"/>
      </xsl:attribute>
    </a>
    <h2>
      <a>
        <xsl:attribute name="href">#<xsl:apply-templates select="." mode="indexname"/></xsl:attribute>
        <xsl:apply-templates select="." mode="fullname"/>
      </a>
    </h2>
    <xsl:apply-templates/>

    <!-- If this type has public fields, display them -->
    <xsl:if test="//member[contains(@name,concat('F:',$FullMemberName,'.'))]">
      <h3>Fields</h3>

      <xsl:for-each select="//member[contains(@name,concat('F:',$FullMemberName))]">
        <div>
          <code>
            .<xsl:value-of select="substring-after(@name, concat('F:',$FullMemberName,'.'))"/>
          </code>
        </div>
      </xsl:for-each>
    </xsl:if>

    <!-- If this type has properties, display them -->
    <xsl:if test="//member[contains(@name,concat('P:',$FullMemberName,'.'))]">
      <h3>Properties</h3>

      <xsl:for-each select="//member[contains(@name,concat('P:',$FullMemberName,'.'))]">
        <div>
          <code>
            .<xsl:value-of select="substring-after(@name, concat('P:',$FullMemberName,'.'))"/>
          </code>
        </div>
      </xsl:for-each>
    </xsl:if>

    <!-- If this type has methods, display them -->
    <xsl:if test="//member[contains(@name,concat('M:',$FullMemberName,'.'))]">
      <H3>Methods</H3>

      <xsl:for-each select="//member[contains(@name,concat('M:',$FullMemberName,'.'))]">

        <!-- If this is a constructor, display the type name 
            (instead of "#ctor"), or display the method name -->
        <div>
          <xsl:choose>
            <xsl:when test="contains(@name, '#ctor')">
              Constructor:
              <xsl:apply-templates select="." mode="fullname"/>
              <xsl:value-of select="substring-after(@name, '#ctor')"/>
            </xsl:when>
            <xsl:otherwise>
              <code>
                .<xsl:value-of select="substring-after(@name, concat('M:',$FullMemberName,'.'))"/>
              </code>
            </xsl:otherwise>
          </xsl:choose>
        </div>

      </xsl:for-each>

    </xsl:if>
  </xsl:template>


  <!-- TYPE TEMPLATE -->
  <!-- Loop through member types and display their properties and methods -->
  <xsl:template match="//member[contains(@name,'T:')]">

    <!-- Two variables to make code easier to read -->
    <!-- A variable for the name of this type -->
    <xsl:variable name="MemberName"
                   select="substring-after(substring-after(substring-after(@name, '.'), '.'), '.')"/>

    <!-- Get the type's fully qualified name without the T: prefix -->
    <xsl:variable name="FullMemberName"
                   select="substring-after(@name, ':')"/>


    <!-- Display the type's name and information -->
    <h2>
      <a>
        <xsl:attribute name="href">
          #<xsl:value-of select="$MemberName"/>
        </xsl:attribute>
        <xsl:value-of select="$MemberName"/>
      </a>
    </h2>
    <xsl:apply-templates/>

    <!-- If this type has public fields, display them -->
    <xsl:if test="//member[contains(@name,concat('F:',$FullMemberName))]">
      <h3>Fields</h3>

      <xsl:for-each select="//member[contains(@name,concat('F:',$FullMemberName))]">
        <H4>
          <xsl:value-of select="substring-after(@name, concat('F:',$FullMemberName,'.'))"/>
        </H4>
        <xsl:apply-templates/>
      </xsl:for-each>
    </xsl:if>

    <!-- If this type has properties, display them -->
    <xsl:if test="//member[contains(@name,concat('P:',$FullMemberName))]">
      <h3>Properties</h3>

      <xsl:for-each select="//member[contains(@name,concat('P:',$FullMemberName))]">
        <H4>
          <xsl:value-of select="substring-after(@name, concat('P:',$FullMemberName,'.'))"/>
        </H4>
        <xsl:apply-templates/>
      </xsl:for-each>
    </xsl:if>

    <!-- If this type has methods, display them -->
    <xsl:if test="//member[contains(@name,concat('M:',$FullMemberName))]">
      <H3>Methods</H3>

      <xsl:for-each select="//member[contains(@name,concat('M:',$FullMemberName))]">

        <!-- If this is a constructor, display the type name 
            (instead of "#ctor"), or display the method name -->
        <H4>
          <xsl:choose>
            <xsl:when test="contains(@name, '#ctor')">
              Constructor:
              <xsl:value-of select="$MemberName"/>
              <xsl:value-of select="substring-after(@name, '#ctor')"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:value-of select="substring-after(@name, concat('M:',$FullMemberName,'.'))"/>
            </xsl:otherwise>
          </xsl:choose>
        </H4>

        <xsl:apply-templates select="summary"/>

        <!-- Display parameters if there are any -->
        <xsl:if test="count(param)!=0">
          <H5>Parameters</H5>
          <xsl:apply-templates select="param"/>
        </xsl:if>

        <!-- Display return value if there are any -->
        <xsl:if test="count(returns)!=0">
          <H5>Return Value</H5>
          <xsl:apply-templates select="returns"/>
        </xsl:if>

        <!-- Display exceptions if there are any -->
        <xsl:if test="count(exception)!=0">
          <H5>Exceptions</H5>
          <xsl:apply-templates select="exception"/>
        </xsl:if>

        <!-- Display examples if there are any -->
        <xsl:if test="count(example)!=0">
          <H5>Example</H5>
          <xsl:apply-templates select="example"/>
        </xsl:if>

      </xsl:for-each>

    </xsl:if>
  </xsl:template>

  <!-- OTHER TEMPLATES -->
  <!-- Templates for other tags -->
  <xsl:template match="c">
    <CODE>
      <xsl:apply-templates />
    </CODE>
  </xsl:template>

  <xsl:template match="code">
    <PRE>
      <xsl:apply-templates />
    </PRE>
  </xsl:template>

  <xsl:template match="example">
    <P>
      <STRONG>Example: </STRONG>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="exception">
    <P>
      <STRONG>
        <xsl:value-of select="substring-after(@cref,'T:')"/>:
      </STRONG>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="include">
    <A HREF="{@file}">External file</A>
  </xsl:template>

  <xsl:template match="para">
    <P>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="param">
    <P>
      <STRONG>
        <xsl:value-of select="@name"/>:
      </STRONG>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="paramref">
    <EM>
      <xsl:value-of select="@name" />
    </EM>
  </xsl:template>

  <xsl:template match="permission">
    <P>
      <STRONG>Permission: </STRONG>
      <EM>
        <xsl:value-of select="@cref" />
      </EM>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="remarks">
    <P>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="returns">
    <P>
      <STRONG>Return Value: </STRONG>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="see">
    <EM>
      See: <xsl:value-of select="@cref" />
    </EM>
  </xsl:template>

  <xsl:template match="seealso">
    <EM>
      See also: <xsl:value-of select="@cref" />
    </EM>
  </xsl:template>

  <xsl:template match="summary">
    <P>
      <xsl:apply-templates />
    </P>
  </xsl:template>

  <xsl:template match="list">
    <xsl:choose>
      <xsl:when test="@type='bullet'">
        <UL>
          <xsl:for-each select="listheader">
            <LI>
              <strong>
                <xsl:value-of select="term"/>:
              </strong>
              <xsl:value-of select="definition"/>
            </LI>
          </xsl:for-each>
          <xsl:for-each select="list">
            <LI>
              <strong>
                <xsl:value-of select="term"/>:
              </strong>
              <xsl:value-of select="definition"/>
            </LI>
          </xsl:for-each>
        </UL>
      </xsl:when>
      <xsl:when test="@type='number'">
        <OL>
          <xsl:for-each select="listheader">
            <LI>
              <strong>
                <xsl:value-of select="term"/>:
              </strong>
              <xsl:value-of select="definition"/>
            </LI>
          </xsl:for-each>
          <xsl:for-each select="list">
            <LI>
              <strong>
                <xsl:value-of select="term"/>:
              </strong>
              <xsl:value-of select="definition"/>
            </LI>
          </xsl:for-each>
        </OL>
      </xsl:when>
      <xsl:when test="@type='table'">
        <TABLE>
          <xsl:for-each select="listheader">
            <TH>
              <TD>
                <xsl:value-of select="term"/>
              </TD>
              <TD>
                <xsl:value-of select="definition"/>
              </TD>
            </TH>
          </xsl:for-each>
          <xsl:for-each select="list">
            <TR>
              <TD>
                <strong>
                  <xsl:value-of select="term"/>:
                </strong>
              </TD>
              <TD>
                <xsl:value-of select="definition"/>
              </TD>
            </TR>
          </xsl:for-each>
        </TABLE>
      </xsl:when>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
