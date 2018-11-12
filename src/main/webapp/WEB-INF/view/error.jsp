<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8"/>
    <title>Action Error Page</title>
</head>

<body>

<h2>Errors</h2>

<c:if test="${!(empty errors)}">
    <c:forEach var="error" items="${errors}">
        <h3 style="color: red; margin-left: 100px;">${error}</h3>
    </c:forEach>
</c:if>

<p>
    Back to <a href="demo.do">demo</a>.
</p>

</body>
</html>