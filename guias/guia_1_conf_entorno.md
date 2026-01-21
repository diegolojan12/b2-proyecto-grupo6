# Configuración del Entorno

Esta guía contiene los pasos técnicos para configurar tu entorno en IntelliJ IDEA, la estructura de carpetas requerida

---

## 1. Configuración del Entorno (IntelliJ IDEA)

Como trabajarás sobre un repositorio base, **no debes crear un proyecto nuevo**, sino clonar el existente.

### Paso 1: Clonar el Repositorio

1. Abre **IntelliJ IDEA**.
2. Selecciona **Get from VCS** (o ve a *File > New > Project from Version Control*).
3. Pega la URL del repositorio proporcionado por el docente y haz clic en **Clone**.

### Paso 2: Inicializar soporte para Scala

Una vez descargado, debes indicarle al IDE que es un proyecto Scala (SBT).

1. Si ves una notificación emergente, haz clic en **Load sbt Script** o **Trust Project**.
2. Si no, haz clic derecho en la carpeta raíz del proyecto → **Add Framework Support...** → Selecciona **Scala** (asegúrate de usar la configuración de clase, usualmente SBT).
3. Esto generará la carpeta `src` y el archivo `build.sbt`.

### Paso 3: Crear el archivo `.gitignore`

Para mantener el repositorio limpio y sin errores, crea un archivo llamado `.gitignore` en la raíz con el siguiente contenido:

```text
.idea/
*.iml
target/
project/target/
.DS_Store
.bsp/
```

---

## 2. Estructura del Proyecto

Debes organizar tu código fuente dentro de `src/main/scala` siguiendo esta estructura para cumplir con los criterios de organización definidos.

```
Nombre_Del_Repositorio/
├── .gitignore
├── README.md                   <-- Documentación técnica principal (EDITAR)
├── build.sbt                   <-- Configuración de dependencias (aquí va play-json)
├── avances/                    <-- Instrucciones de entrega (Referencia)
│   ├── avance_1.md
│   ├── avance_2.md
│   └── criterios_calificación.md
└── src/
    └── main/
        ├── resources/          <-- Archivos de datos (.csv, .json)
        └── scala/
            ├── models/         <-- Case Classes y estructuras de datos
            ├── utilities/      <-- Limpieza de datos y funciones de ayuda
            ├── data/           <-- Lectura de archivos y consultas a BD
            └── Main.scala      <-- Ejecución principal
```

---

