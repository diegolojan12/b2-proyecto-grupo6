package models

case class Collection(
                       id: Option[Int],
                       name: Option[String],
                       poster_path: Option[String],
                       backdrop_path: Option[String]
                     )

case class Genre(
                  id: Option[Int],
                  name: Option[String]
                )

case class ProductionCompany(
                              id: Option[Int],
                              name: Option[String]
                            )

case class ProductionCountry(
                              iso_3166_1: Option[String],
                              name: Option[String]
                            )

case class SpokenLanguage(
                           iso_639_1: Option[String],
                           name: Option[String]
                         )

case class Keyword(
                    id: Option[Int],
                    name: Option[String]
                  )

case class CastMember(
                       cast_id: Option[Int],
                       character: Option[String],
                       credit_id: Option[String],
                       gender: Option[Int],
                       id: Option[Int],
                       name: Option[String],
                       order: Option[Int],
                       profile_path: Option[String]
                     )

case class CrewMember(
                       credit_id: Option[String],
                       department: Option[String],
                       gender: Option[Int],
                       id: Option[Int],
                       job: Option[String],
                       name: Option[String],
                       profile_path: Option[String]
                     )

case class Rating(
                   userId: Option[Int],
                   rating: Option[Double],
                   timestamp: Option[Long]
                 )

case class MovieData(
                      adult: Option[String],
                      budget: Option[BigDecimal],
                      homepage: Option[String],
                      id: Option[Int],
                      imdb_id: Option[String],
                      original_language: Option[String],
                      original_title: Option[String],
                      overview: Option[String],
                      popularity: Option[BigDecimal],
                      poster_path: Option[String],
                      release_date: Option[String],
                      revenue: Option[BigDecimal],
                      runtime: Option[BigDecimal],
                      status: Option[String],
                      tagline: Option[String],
                      title: Option[String],
                      video: Option[String],
                      vote_average: Option[BigDecimal],
                      vote_count: Option[Int]
                    )

case class FilaCompleta(
                         movieData: MovieData,
                         collectionLimpia: Option[Collection],
                         genresLimpios: List[Genre],
                         productionCompaniesLimpias: List[ProductionCompany],
                         productionCountriesLimpios: List[ProductionCountry],
                         spokenLanguagesLimpios: List[SpokenLanguage],
                         keywordsLimpios: List[Keyword],
                         castLimpio: List[CastMember],
                         crewLimpia: List[CrewMember],
                         ratingsLimpios: List[Rating]
                       )
case class CalidadColumna(
                           columna: String,
                           total: Int,
                           nulos: Int,
                           ceros: Int,
                           negativos: Int,
                           vacios: Int,
                           porcentajeValidos: Double
                         )

case class Movie(
                  adult: String,
                  belongs_to_collection: String,
                  budget: Double,
                  genres: String,
                  homepage: String,
                  id: Double,
                  imdb_id: String,
                  original_language: String,
                  original_title: String,
                  overview: String,
                  popularity: Double,
                  poster_path: String,
                  production_companies: String,
                  production_countries: String,
                  release_date: String,
                  revenue: Double,
                  runtime: Double,
                  spoken_languages: String,
                  status: String,
                  tagline: String,
                  title: String,
                  video: String,
                  vote_average: Double,
                  vote_count: Double,
                  release_year: Double,
                  release_month: Double,
                  release_day: Double,
                  `return`: Double
                )

case class MovieNumeric(
                         id: Double,
                         budget: Double,
                         popularity: Double,
                         revenue: Double,
                         runtime: Double,
                         vote_average: Double,
                         vote_count: Double
                       )

case class MovieRaw(
                     adult: String,
                     belongs_to_collection: String,
                     budget: Double,
                     genres: String,
                     homepage: String,
                     id: Double,
                     imdb_id: String,
                     original_language: String,
                     original_title: String,
                     overview: String,
                     popularity: Double,
                     poster_path: String,
                     production_companies: String,
                     production_countries: String,
                     release_date: String,
                     revenue: Double,
                     runtime: Double,
                     spoken_languages: String,
                     status: String,
                     tagline: String,
                     title: String,
                     video: String,
                     vote_average: Double,
                     vote_count: Double
                   )

case class MovieText(
                      belongs_to_collection: String,
                      original_language: String,
                      status: String,
                      tagline: String,
                      title: String
                    )